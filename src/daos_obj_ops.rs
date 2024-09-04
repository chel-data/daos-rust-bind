//
//  Copyright (C) 2024 github.com/chel-data
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <https://www.gnu.org/licenses/>.
//

use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::ptr;

use crate::async_utils::*;
use crate::bindings::{
    d_iov_t, d_sg_list_t, daos_event_t, daos_handle_t, daos_iod_t, daos_iod_type_t_DAOS_IOD_SINGLE,
    daos_key_t, daos_obj_fetch, daos_obj_generate_oid2, daos_obj_id_t, daos_obj_open,
    daos_obj_punch, daos_obj_update, daos_oclass_hints_t, daos_oclass_id_t, daos_otype_t,
    daos_otype_t_DAOS_OT_MULTI_HASHED, DAOS_OO_RO, DAOS_OO_RW, DAOS_REC_ANY, DAOS_TXN_NONE,
    OC_UNKNOWN,
};
use crate::daos::{DaosContainer, DaosObject, DaosTxn};
use std::task::{Context, Poll};

pub trait DasoObjSyncOps {
    fn create(
        cont: &DaosContainer<'_>,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> Result<Box<DaosObject>>;
    fn open(
        cont: &DaosContainer<'_>,
        oid: daos_obj_id_t,
        read_only: bool,
    ) -> Result<Box<DaosObject>>;
    fn punch(&self) -> Result<()>;
}

pub trait DaosObjAsyncOps {
    fn create_async(
        cont: &DaosContainer<'_>,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static;
    fn open_async(
        cont: &DaosContainer<'_>,
        oid: daos_obj_id_t,
        read_only: bool,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static;
    fn punch_async(&self, txn: &DaosTxn) -> impl Future<Output = Result<()>> + Send + 'static;
    fn fetch_async(
        &self,
        txn: &DaosTxn,
        flags: u64,
        dkey: Vec<u8>,
        akey: Vec<u8>,
        max_size: u32,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send + 'static;
    fn update_async(
        &self,
        txn: &DaosTxn,
        flags: u64,
        dkey: Vec<u8>,
        akey: Vec<u8>,
        data: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send + 'static;
}

impl DasoObjSyncOps for DaosObject {
    fn create(
        cont: &DaosContainer<'_>,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> Result<Box<DaosObject>> {
        Err(Error::new(ErrorKind::Other, "Not implemented"))
    }
    fn open(
        cont: &DaosContainer<'_>,
        oid: daos_obj_id_t,
        read_only: bool,
    ) -> Result<Box<DaosObject>> {
        Err(Error::new(ErrorKind::Other, "Not implemented"))
    }
    fn punch(&self) -> Result<()> {
        Err(Error::new(ErrorKind::Other, "Not implemented"))
    }
}

impl DaosObjAsyncOps for DaosObject {
    fn create_async(
        cont: &DaosContainer<'_>,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static {
        let eq = cont.get_event_queue();
        let cont_hdl = cont.get_handle();
        async move {
            let mut oid = daos_obj_id_t { lo: 0, hi: 0 };
            let ret =
                unsafe { daos_obj_generate_oid2(cont_hdl, &mut oid, otype, cid, hints, args) };

            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't generate object id"));
            }

            let res = create_async_event(eq);
            if res.is_err() {
                return Err(res.unwrap_err());
            }

            let (mut event, _call_arg, rx) = res.unwrap();

            let mut obj_hdl = daos_handle_t { cookie: 0u64 };
            let ret = unsafe {
                daos_obj_open(
                    cont_hdl,
                    oid,
                    0,
                    &mut obj_hdl,
                    event.as_mut() as *mut daos_event_t,
                )
            };

            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't open object"));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        return Err(Error::new(ErrorKind::Other, "async open operation fail"));
                    }
                }
                Err(_) => {
                    return Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early"));
                }
            }

            Ok(Box::new(DaosObject::new(oid, obj_hdl, Some(eq))))
        }
    }

    fn open_async(
        cont: &DaosContainer<'_>,
        oid: daos_obj_id_t,
        read_only: bool,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static {
        let eq = cont.get_event_queue();
        let cont_hdl = cont.get_handle();
        async move {
            let res = create_async_event(eq);
            if res.is_err() {
                return Err(res.unwrap_err());
            }

            let (mut event, _call_arg, rx) = res.unwrap();

            let mut obj_hdl = daos_handle_t { cookie: 0u64 };
            let ret = unsafe {
                daos_obj_open(
                    cont_hdl,
                    oid,
                    if read_only { DAOS_OO_RO } else { DAOS_OO_RW },
                    &mut obj_hdl,
                    event.as_mut() as *mut daos_event_t,
                )
            };

            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't open object"));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(ErrorKind::Other, "async open object fail"))
                    } else {
                        Ok(Box::new(DaosObject::new(oid, obj_hdl, Some(eq))))
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early")),
            }
        }
    }

    fn punch_async(&self, txn: &DaosTxn) -> impl Future<Output = Result<()>> + Send + 'static {
        let eq = self.get_event_queue().clone();
        let obj_hdl = self.get_handle().clone();
        let tx_hdl = txn.get_handle().clone();
        async move {
            if eq.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "event queue is nil"));
            }
            if obj_hdl.is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "punch uninitialized object",
                ));
            }

            let res = create_async_event(eq.unwrap());
            let (mut event, _call_arg, rx) = match res {
                Ok(res) => res,
                Err(e) => return Err(e),
            };

            let txn = match tx_hdl {
                Some(tx) => tx,
                None => DAOS_TXN_NONE,
            };

            let ret = unsafe { daos_obj_punch(obj_hdl.unwrap(), txn, 0, event.as_mut()) };
            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't punch object"));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(ErrorKind::Other, "async punch operation fail"))
                    } else {
                        Ok(())
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early")),
            }
        }
    }

    fn fetch_async(
        &self,
        txn: &DaosTxn,
        flags: u64,
        dkey: Vec<u8>,
        akey: Vec<u8>,
        max_size: u32,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send + 'static {
        let eq = self.get_event_queue().clone();
        let obj_hdl = self.get_handle().clone();
        let tx_hdl = txn.get_handle().clone();
        async move {
            if eq.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "event queue is nil"));
            }
            if obj_hdl.is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "fetch uninitialized object",
                ));
            }

            let res = create_async_event(eq.unwrap());
            let (mut event, _call_arg, rx) = match res {
                Ok(res) => res,
                Err(e) => return Err(e),
            };

            let txn = match tx_hdl {
                Some(tx) => tx,
                None => DAOS_TXN_NONE,
            };

            let mut dkey_wrapper = daos_key_t {
                iov_buf: dkey.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                iov_buf_len: dkey.len(),
                iov_len: dkey.len(),
            };
            let mut iod = daos_iod_t {
                iod_name: daos_key_t {
                    iov_buf: akey.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                    iov_buf_len: akey.len(),
                    iov_len: akey.len(),
                },
                iod_type: daos_iod_type_t_DAOS_IOD_SINGLE,
                iod_size: DAOS_REC_ANY as u64,
                iod_flags: 0,
                iod_nr: 1,
                iod_recxs: std::ptr::null_mut(),
            };
            let mut buf = Vec::with_capacity(max_size as usize);
            buf.resize(max_size as usize, 0u8);
            let mut sg_iov = d_iov_t {
                iov_buf: buf.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                iov_buf_len: buf.len(),
                iov_len: buf.len(),
            };
            let mut sgl = d_sg_list_t {
                sg_nr: 1,
                sg_nr_out: 0,
                sg_iovs: &mut sg_iov,
            };
            let ret = unsafe {
                daos_obj_fetch(
                    obj_hdl.unwrap(),
                    txn,
                    flags,
                    &mut dkey_wrapper,
                    1,
                    &mut iod,
                    &mut sgl,
                    ptr::null_mut(),
                    event.as_mut(),
                )
            };
            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't fetch object"));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(ErrorKind::Other, "async fetch operation fail"))
                    } else {
                        buf.resize(iod.iod_size as usize, 0xffu8);
                        Ok(buf)
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early")),
            }
        }
    }

    fn update_async(
        &self,
        txn: &DaosTxn,
        flags: u64,
        dkey: Vec<u8>,
        akey: Vec<u8>,
        data: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let eq = self.get_event_queue().clone();
        let obj_hdl = self.get_handle().clone();
        let tx_hdl = txn.get_handle().clone();
        async move {
            if eq.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "event queue is nil"));
            }
            if obj_hdl.is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "update uninitialized object",
                ));
            }

            let res = create_async_event(eq.unwrap());
            let (mut event, _call_arg, rx) = match res {
                Ok(res) => res,
                Err(e) => return Err(e),
            };

            let txn = match tx_hdl {
                Some(tx) => tx,
                None => DAOS_TXN_NONE,
            };

            let mut dkey_wrapper = daos_key_t {
                iov_buf: dkey.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                iov_buf_len: dkey.len(),
                iov_len: dkey.len(),
            };
            let mut iod = daos_iod_t {
                iod_name: daos_key_t {
                    iov_buf: akey.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                    iov_buf_len: akey.len(),
                    iov_len: akey.len(),
                },
                iod_type: daos_iod_type_t_DAOS_IOD_SINGLE,
                iod_size: data.len() as u64,
                iod_flags: 0,
                iod_nr: 1,
                iod_recxs: std::ptr::null_mut(),
            };
            let mut sg_iov = d_iov_t {
                iov_buf: data.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                iov_buf_len: data.len(),
                iov_len: data.len(),
            };
            let mut sgl = d_sg_list_t {
                sg_nr: 1,
                sg_nr_out: 0,
                sg_iovs: &mut sg_iov,
            };
            let ret = unsafe {
                daos_obj_update(
                    obj_hdl.unwrap(),
                    txn,
                    flags,
                    &mut dkey_wrapper,
                    1,
                    &mut iod,
                    &mut sgl,
                    event.as_mut(),
                )
            };
            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't update object"));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(ErrorKind::Other, "async update operation fail"))
                    } else {
                        Ok(())
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::JoinHandle;

    use super::*;
    use tokio::task;

    use crate::daos::DaosPool;

    const TEST_POOL_NAME: &str = "pool1";
    const TEST_CONT_NAME: &str = "cont1";

    #[test]
    fn test_create_async() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut pool = DaosPool::new(TEST_POOL_NAME);
                pool.connect().expect("Failed to connect to pool");

                let mut cont = DaosContainer::new(TEST_CONT_NAME, &pool);
                cont.connect().expect("Failed to connect to container");

                let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
                let cid: daos_oclass_id_t = OC_UNKNOWN;
                let hints: daos_oclass_hints_t = 0;
                let args = 0;

                let result = DaosObject::create_async(&cont, otype, cid, hints, args).await;

                assert!(result.is_ok());
                let obj_box = result.unwrap();
                // Assert obj_box is created correctly
            });
    }

    #[test]
    fn test_open_async() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut pool = DaosPool::new(TEST_POOL_NAME);
                pool.connect().expect("Failed to connect to pool");

                let mut cont = DaosContainer::new(TEST_CONT_NAME, &pool);
                cont.connect().expect("Failed to connect to container");

                let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
                let cid: daos_oclass_id_t = OC_UNKNOWN;
                let hints: daos_oclass_hints_t = 0;
                let args = 0;

                let result = DaosObject::create_async(&cont, otype, cid, hints, args).await;
                assert!(result.is_ok());
                let obj_box = result.unwrap();

                let oid = obj_box.oid;

                let result = DaosObject::open_async(&cont, oid, /* read_only */ true).await;
                assert!(result.is_ok());
                let obj = result.unwrap();
                // Assert obj is opened correctly
            });
    }

    #[test]
    fn test_punch_async() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut pool = DaosPool::new(TEST_POOL_NAME);
                pool.connect().expect("Failed to connect to pool");

                let mut cont = DaosContainer::new(TEST_CONT_NAME, &pool);
                cont.connect().expect("Failed to connect to container");

                let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
                let cid: daos_oclass_id_t = OC_UNKNOWN;
                let hints: daos_oclass_hints_t = 0;
                let args = 0;

                let result = DaosObject::create_async(&cont, otype, cid, hints, args).await;
                assert!(result.is_ok());
                let obj_box = result.unwrap();

                let txn = DaosTxn::txn_none();
                let result = obj_box.punch_async(&txn).await;
                assert!(result.is_ok());
                // Assert punch operation is successful
            });
    }

    #[test]
    fn test_fetch_async() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut pool = DaosPool::new(TEST_POOL_NAME);
                pool.connect().expect("Failed to connect to pool");

                let mut cont = DaosContainer::new(TEST_CONT_NAME, &pool);
                cont.connect().expect("Failed to connect to container");

                let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
                let cid: daos_oclass_id_t = OC_UNKNOWN;
                let hints: daos_oclass_hints_t = 0;
                let args = 0;

                let result = DaosObject::create_async(&cont, otype, cid, hints, args).await;
                assert!(result.is_ok());
                let obj_box = result.unwrap();

                let txn = DaosTxn::txn_none();
                let flags = 0;
                let dkey = vec![0u8, 1u8, 2u8, 3u8];
                let akey = vec![0u8];
                let max = 1024;
                let result = obj_box.fetch_async(&txn, flags, dkey, akey, max).await;
                assert!(result.is_ok());
                let data = result.unwrap();
                // Assert fetched data is correct
            });
    }

    #[test]
    fn test_update_async() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut pool = DaosPool::new(TEST_POOL_NAME);
                pool.connect().expect("Failed to connect to pool");

                let mut cont = DaosContainer::new(TEST_CONT_NAME, &pool);
                cont.connect().expect("Failed to connect to container");

                let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
                let cid: daos_oclass_id_t = OC_UNKNOWN;
                let hints: daos_oclass_hints_t = 0;
                let args = 0;

                let result = DaosObject::create_async(&cont, otype, cid, hints, args).await;
                assert!(result.is_ok());
                let obj_box = result.unwrap();

                let txn = DaosTxn::txn_none();
                let flags = 0;
                let dkey = vec![0u8, 1u8, 2u8, 3u8];
                let akey = vec![0u8];
                let data = vec![1u8; 256];
                let result = obj_box.update_async(&txn, flags, dkey, akey, data).await;
                assert!(result.is_ok());
                // Assert update operation is successful
            });
    }
}
