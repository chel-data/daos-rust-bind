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

use crate::bindings::{
    daos_handle_t, daos_init, daos_obj_id_t, daos_pool_connect2, daos_pool_disconnect, DAOS_PC_RW,
};
use std::ffi::CString;
use std::sync::Once;
use std::{
    io::{Error, ErrorKind, Result},
    option::Option,
    ptr,
};

pub type DaosHandle = daos_handle_t;
pub type DaosObjectId = daos_obj_id_t;

static INIT_DAOS: Once = Once::new();

#[derive(Debug)]
pub struct DaosPool {
    pub label: String,
    handle: Option<DaosHandle>,
}

impl DaosPool {
    pub fn new(label: &str) -> Self {
        INIT_DAOS.call_once(|| unsafe {
            daos_init();
        });

        DaosPool {
            label: label.to_string(),
            handle: None,
        }
    }

    pub(crate) fn get_handle(&self) -> Option<DaosHandle> {
        self.handle.clone()
    }

    // Should not be called in async executer like tokio.
    // Consider spawning a new thread to open/close pools.
    pub fn connect(&mut self) -> Result<()> {
        if self.handle.is_some() {
            return Ok(());
        }

        let c_label = CString::new(self.label.clone()).unwrap();
        let mut poh: DaosHandle = DaosHandle { cookie: 0u64 };
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

#[cfg(test)]
mod tests {
    use super::*;
    const TEST_POOL_NAME: &str = "pool1";

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
}
