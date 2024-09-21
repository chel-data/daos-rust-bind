/*
 *  Copyright (C) 2024 github.com/chel-data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use crate::daos_event::*;
use crate::bindings::{
    daos_cont_close, daos_cont_open2, daos_cont_props_DAOS_PROP_CO_ROOTS, daos_cont_query, daos_prop_alloc, daos_prop_co_roots, daos_prop_entry_get,
    daos_prop_free, daos_prop_t, DAOS_COO_RW,
};
use crate::daos_pool::{DaosHandle, DaosObjectId, DaosPool};
use std::ffi::CString;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::ptr;

#[derive(Debug)]
pub struct DaosProperty {
    raw_prop: Option<*mut daos_prop_t>,
}

unsafe impl Send for DaosProperty {}

impl DaosProperty {
    fn new() -> Result<Self> {
        let prop = unsafe { daos_prop_alloc(1) };
        if !prop.is_null() {
            unsafe { (*(*prop).dpp_entries).dpe_type = daos_cont_props_DAOS_PROP_CO_ROOTS; }
            Ok(DaosProperty {
                raw_prop: Some(prop),
            })
        } else {
            Err(Error::new(
                ErrorKind::Other,
                "Failed to allocate DAOS property",
            ))
        }
    }

    pub fn get_co_roots(&self) -> Result<Box<[DaosObjectId; 4]>> {
        let entry = unsafe {
            daos_prop_entry_get(
                self.raw_prop.clone().unwrap(),
                daos_cont_props_DAOS_PROP_CO_ROOTS,
            )
        };
        if entry.is_null() {
            return Err(Error::new(
                ErrorKind::Other,
                "Failed to get a CO roots prop entry",
            ));
        }

        let raw_roots = unsafe { (*entry).__bindgen_anon_1.dpe_val_ptr as *mut daos_prop_co_roots };

        if raw_roots.is_null() {
            return Err(Error::new(
                ErrorKind::Other,
                "empty CO roots in the prop entry",
            ));
        }

        let roots = Box::new(unsafe { (*raw_roots).cr_oids });
        Ok(roots)
    }
}

impl Drop for DaosProperty {
    fn drop(&mut self) {
        if self.raw_prop.is_some() {
            unsafe {
                daos_prop_free(self.raw_prop.unwrap());
            }
        }
    }
}

pub trait DaosContainerSyncOps {
    fn query_prop(&self) -> Result<DaosProperty>;
}

pub trait DaosContainerAsyncOps {
    fn query_prop_async(&self) -> impl Future<Output = Result<DaosProperty>> + Send + 'static;
}

#[derive(Debug)]
pub struct DaosContainer {
    pub label: String,
    handle: Option<DaosHandle>,
    event_queue: Option<DaosEventQueue>,
}

impl DaosContainer {
    pub fn new(label: &str) -> Self {
        DaosContainer {
            label: label.to_string(),
            handle: None,
            event_queue: None,
        }
    }

    pub fn get_handle(&self) -> Option<DaosHandle> {
        self.handle.clone()
    }

    pub fn get_event_queue(&self) -> Option<&DaosEventQueue> {
        self.event_queue.as_ref()
    }

    // Should not be called in async executer like tokio.
    // Consider spawning a new thread to open/close containers.
    pub fn connect(&mut self, daos_pool: &DaosPool) -> Result<()> {
        if self.handle.is_some() {
            return Ok(());
        }

        if daos_pool.get_handle().is_none() {
            return Err(Error::new(ErrorKind::Other, "Pool is not connected"));
        }

        let c_label = CString::new(self.label.clone()).unwrap();
        let mut coh: DaosHandle = DaosHandle { cookie: 0u64 };
        let res = unsafe {
            daos_cont_open2(
                daos_pool.get_handle().unwrap(),
                c_label.as_ptr(),
                DAOS_COO_RW,
                &mut coh,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if res == 0 {
            self.handle.replace(coh);
            self.create_eq()
        } else {
            Err(Error::new(
                ErrorKind::Other,
                "Failed to open DAOS container",
            ))
        }
    }

    // Should not be called in async executer like tokio.
    // Consider spawning a new thread to open/close pools.
    pub fn disconnect(&mut self) -> Result<()> {
        if self.handle.is_some() {
            let res = unsafe { daos_cont_close(self.handle.unwrap(), ptr::null_mut()) };
            if res == 0 {
                self.handle.take();
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::Other,
                    "Failed to close DAOS container",
                ))
            }
        } else {
            Ok(())
        }
    }

    fn create_eq(&mut self) -> Result<()> {
        if self.event_queue.is_some() {
            return Ok(());
        }

        let res = DaosEventQueue::new();
        match res {
            Ok(eqh) => {
                self.event_queue.replace(eqh);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Drop for DaosContainer {
    fn drop(&mut self) {
        let res = self.disconnect();
        match res {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to drop DAOS container: {:?}", e);
            }
        }
    }
}

impl DaosContainerAsyncOps for DaosContainer {
    fn query_prop_async(&self) -> impl Future<Output = Result<DaosProperty>> + Send + 'static {
        let cont_hdl = self.handle.clone();
        let eq = self.get_event_queue();
        let ev = eq.map(|e| e.create_event());

        async move {
            if ev.is_none() {
                return Err(Error::new(ErrorKind::InvalidInput, "empty event queue"));
            }
            let mut event = ev.unwrap()?;

            let rx = event.register_callback()?;

            let prop = DaosProperty::new()?;

            let ret = unsafe {
                daos_cont_query(
                    cont_hdl.unwrap(),
                    ptr::null_mut(),
                    prop.raw_prop.clone().unwrap(),
                    event.as_mut(),
                )
            };

            if ret != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Failed to query DAOS container",
                ));
            }

            match rx.await {
                Ok(res) => {
                    if res != 0 {
                        Err(Error::new(ErrorKind::Other, "async query container failed"))
                    } else {
                        Ok(prop)
                    }
                }
                Err(_) => Err(Error::new(
                    ErrorKind::Other,
                    "can't get response from the receiver",
                )),
            }
        }
    }
}

impl DaosContainerSyncOps for DaosContainer {
    fn query_prop(&self) -> Result<DaosProperty> {
        let prop = DaosProperty::new()?;
        let ret = unsafe {
            daos_cont_query(
                self.handle.clone().unwrap(),
                ptr::null_mut(),
                prop.raw_prop.clone().unwrap(),
                ptr::null_mut(),
            )
        };
        if ret != 0 {
            return Err(Error::new(
                ErrorKind::Other,
                "Failed to query DAOS container",
            ));
        }
        Ok(prop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daos_pool::DaosPool;
    use tokio;
    const TEST_POOL_NAME: &str = "pool1";
    const TEST_CONT_NAME: &str = "cont1";

    #[test]
    fn test_daos_container_connect() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        let result = pool.connect();
        assert_eq!(result.is_ok(), true);

        let mut container = DaosContainer::new(TEST_CONT_NAME);
        assert_eq!(container.handle.is_some(), false);

        let result = container.connect(&pool);
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), true);

        let result = container.connect(&pool);
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), true);

        let result = container.disconnect();
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_daos_container_disconnect() {
        let _pool = DaosPool::new(TEST_POOL_NAME);
        let mut container = DaosContainer::new(TEST_CONT_NAME);
        assert_eq!(container.handle.is_some(), false);

        let result = container.disconnect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), false);

        let result = container.disconnect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), false);
    }

    #[test]
    fn test_query_cont_prop() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        let result = pool.connect();
        assert_eq!(result.is_ok(), true);

        let mut container = DaosContainer::new(TEST_CONT_NAME);
        let result = container.connect(&pool);
        assert_eq!(result.is_ok(), true);

        let prop = container.query_prop();
        assert_eq!(prop.is_ok(), true);
    }

    #[tokio::test]
    async fn test_async_query_cont_prop() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        let result = pool.connect();
        assert_eq!(result.is_ok(), true);

        let mut container = DaosContainer::new(TEST_CONT_NAME);
        let result = container.connect(&pool);
        assert_eq!(result.is_ok(), true);

        let prop = container.query_prop_async().await;
        assert_eq!(prop.is_ok(), true);
    }
}
