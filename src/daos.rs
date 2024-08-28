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

use std::ffi::CString;
use std::{
    io::{Error, ErrorKind, Result},
    option::Option,
    ptr,
};

use crate::bindings::{
    daos_cont_close, daos_cont_open2, daos_eq_create, daos_eq_destroy, daos_handle_t,
    daos_obj_close, daos_obj_id_t, daos_pool_connect2, daos_pool_disconnect, DAOS_COO_RW,
    DAOS_PC_RW,
};

#[derive(Debug)]
pub struct DaosPool {
    pub label: String,
    handle: Option<daos_handle_t>,
}

impl DaosPool {
    pub fn new(label: &str) -> Self {
        DaosPool {
            label: label.to_string(),
            handle: None,
        }
    }

    // Should not be called in async executer like tokio.
    // Consider spawning a new thread to open/close pools.
    pub fn connect(&mut self) -> Result<()> {
        if self.handle.is_some() {
            return Ok(());
        }

        let c_label = CString::new(self.label.clone()).unwrap();
        let mut poh: daos_handle_t = daos_handle_t { cookie: 0u64 };
        let res = unsafe {
            daos_pool_connect2(
                c_label.as_ptr(),
                ptr::null(),
                DAOS_PC_RW,
                &mut poh,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if res == 0 {
            self.handle.replace(poh);
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Other,
                "Failed to connect to DAOS pool",
            ))
        }
    }

    // Should not be called in async executer like tokio.
    // Consider spawning a new thread to open/close pools.
    pub fn disconnect(&mut self) -> Result<()> {
        if self.handle.is_some() {
            let res = unsafe { daos_pool_disconnect(self.handle.unwrap(), ptr::null_mut()) };
            if res == 0 {
                self.handle.take();
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::Other,
                    "Failed to disconnect from DAOS pool",
                ))
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for DaosPool {
    fn drop(&mut self) {
        let res = self.disconnect();
        match res {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to disconnect from DAOS pool: {:?}", e);
            }
        }
    }
}

#[derive(Debug)]
pub struct DaosContainer<'a> {
    pub label: String,
    pool: &'a DaosPool,
    handle: Option<daos_handle_t>,
    event_queue: Option<daos_handle_t>,
}

impl<'a> DaosContainer<'a> {
    pub fn new(label: &str, daos_pool: &'a DaosPool) -> Self {
        DaosContainer {
            label: label.to_string(),
            pool: daos_pool,
            handle: None,
            event_queue: None,
        }
    }

    pub fn get_handle(&self) -> daos_handle_t {
        self.handle.unwrap()
    }

    pub fn get_event_queue(&self) -> daos_handle_t {
        self.event_queue.unwrap()
    }

    // Should not be called in async executer like tokio.
    // Consider spawning a new thread to open/close containers.
    pub fn connect(&mut self) -> Result<()> {
        if self.handle.is_some() {
            return Ok(());
        }

        if self.pool.handle.is_none() {
            return Err(Error::new(ErrorKind::Other, "Pool is not connected"));
        }

        let c_label = CString::new(self.label.clone()).unwrap();
        let mut coh: daos_handle_t = daos_handle_t { cookie: 0u64 };
        let res = unsafe {
            daos_cont_open2(
                self.pool.handle.unwrap(),
                c_label.as_ptr(),
                DAOS_COO_RW,
                &mut coh,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if res == 0 {
            self.handle.replace(coh);
            self.create_event_queue()
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
            }
        }

        if self.event_queue.is_some() {
            self.destroy_event_queu()?;
        }

        if self.handle.is_some() || self.event_queue.is_some() {
            Err(Error::new(
                ErrorKind::Other,
                "Failed to close DAOS container",
            ))
        } else {
            Ok(())
        }
    }

    fn create_event_queue(&mut self) -> Result<()> {
        if self.event_queue.is_some() {
            return Ok(());
        }

        let mut eqh: daos_handle_t = daos_handle_t { cookie: 0u64 };
        let res = unsafe { daos_eq_create(&mut eqh) };
        if res == 0 {
            self.event_queue.replace(eqh);
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Other,
                "Failed to create DAOS event queue",
            ))
        }
    }

    fn destroy_event_queu(&mut self) -> Result<()> {
        if self.event_queue.is_none() {
            return Ok(());
        }

        let res = unsafe { daos_eq_destroy(self.event_queue.unwrap(), 0) };
        if res == 0 {
            self.event_queue.take();
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Other,
                "Failed to destroy DAOS event queue",
            ))
        }
    }
}

impl Drop for DaosContainer<'_> {
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

pub struct DaosObject {
    pub oid: daos_obj_id_t,
    handle: Option<daos_handle_t>,
}

impl DaosObject {
    pub fn new(id: daos_obj_id_t, hdl: daos_handle_t) -> Self {
        DaosObject {
            oid: id,
            handle: Some(hdl),
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.handle.is_some() {
            let res = unsafe { daos_obj_close(self.handle.unwrap(), ptr::null_mut()) };
            if res == 0 {
                self.handle.take();
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::Other,
                    "Failed to close DAOS object",
                ))
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for DaosObject {
    fn drop(&mut self) {
        let res = self.close();
        match res {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to drop DAOS object: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const TEST_POOL_NAME: &str = "pool1";
    const TEST_CONT_NAME: &str = "cont1";

    #[test]
    fn test_daos_pool_connect() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        assert_eq!(pool.handle.is_some(), false);

        let result = pool.connect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(pool.handle.is_some(), true);

        let result = pool.connect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(pool.handle.is_some(), true);

        let result = pool.disconnect();
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_daos_pool_disconnect() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        assert_eq!(pool.handle.is_some(), false);

        let result = pool.disconnect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(pool.handle.is_some(), false);

        let result = pool.disconnect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(pool.handle.is_some(), false);
    }

    #[test]
    fn test_daos_container_connect() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        let mut container = DaosContainer::new(TEST_CONT_NAME, &pool);
        assert_eq!(container.handle.is_some(), false);

        let result = container.connect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), true);

        let result = container.connect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), true);

        let result = container.disconnect();
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_daos_container_disconnect() {
        let mut pool = DaosPool::new("my_pool");
        let mut container = DaosContainer::new("my_container", &pool);
        assert_eq!(container.handle.is_some(), false);

        let result = container.disconnect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), false);

        let result = container.disconnect();
        assert_eq!(result.is_ok(), true);
        assert_eq!(container.handle.is_some(), false);
    }
}
