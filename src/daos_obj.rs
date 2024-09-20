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

use crate::daos_event::*;
use crate::bindings::{
    d_iov_t, d_sg_list_t, daos_anchor_is_eof, daos_anchor_t, daos_event_t, daos_handle_t,
    daos_iod_t, daos_iod_type_t_DAOS_IOD_SINGLE, daos_key_desc_t, daos_key_t, daos_obj_fetch,
    daos_obj_generate_oid2, daos_obj_id_t, daos_obj_list_dkey, daos_obj_open, daos_obj_punch,
    daos_obj_update, daos_oclass_hints_t, daos_oclass_id_t, daos_otype_t, DAOS_ANCHOR_BUF_MAX,
    DAOS_OO_RO, DAOS_OO_RW, DAOS_REC_ANY, DAOS_TXN_NONE, daos_obj_close,
};
use crate::daos_cont::DaosContainer;
use crate::daos_txn::DaosTxn;
use std::cmp::{Eq, PartialEq};
use std::future::Future;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::{Error, ErrorKind, Result};
use std::ptr;
use std::vec::Vec;

const MAX_KEY_DESCS: u32 = 128;
const KEY_BUF_SIZE: usize = 1024;

pub const DAOS_OT_ARRAY_BYTE: daos_otype_t = crate::bindings::daos_otype_t_DAOS_OT_ARRAY_BYTE;
pub const DAOS_OC_UNKNOWN: daos_oclass_id_t = crate::bindings::OC_UNKNOWN;
pub const DAOS_OC_HINTS_NONE: daos_oclass_hints_t = 0;
pub const DAOS_COND_DKEY_INSERT: u32 = crate::bindings::DAOS_COND_DKEY_INSERT;
pub const DAOS_COND_DKEY_UPDATE: u32 = crate::bindings::DAOS_COND_DKEY_UPDATE;

pub type DaosObjectId = daos_obj_id_t;

impl Hash for daos_obj_id_t {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.lo.hash(state);
        self.hi.hash(state);
    }
}

impl PartialEq for daos_obj_id_t {
    fn eq(&self, other: &Self) -> bool {
        self.lo == other.lo && self.hi == other.hi
    }
    fn ne(&self, other: &Self) -> bool {
        self.lo != other.lo || self.hi != other.hi
    }
}

impl Eq for daos_obj_id_t {}

#[derive(Debug)]
pub struct DaosObject {
    pub oid: daos_obj_id_t,
    handle: Option<daos_handle_t>,
    event_que: Option<daos_handle_t>,
}

impl DaosObject {
    pub(crate) fn new(
        id: daos_obj_id_t,
        hdl: daos_handle_t,
        evt_que: Option<daos_handle_t>,
    ) -> Self {
        DaosObject {
            oid: id,
            handle: Some(hdl),
            event_que: evt_que,
        }
    }

    pub fn get_handle(&self) -> Option<daos_handle_t> {
        self.handle.clone()
    }

    pub fn get_event_queue(&self) -> &Option<daos_handle_t> {
        &self.event_que
    }

    fn close(&mut self) -> Result<()> {
        if self.handle.is_some() {
            let res = unsafe { daos_obj_close(self.handle.unwrap(), ptr::null_mut()) };
            if res == 0 {
                self.handle.take();
                Ok(())
            } else {
                Err(Error::new(ErrorKind::Other, "Failed to close DAOS object"))
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

#[derive(Debug)]
pub struct DaosKeyList {
    anchor: Box<daos_anchor_t>,
    ndesc: Box<u32>,
    key_descs: Vec<daos_key_desc_t>,
    out_buf: Vec<u8>,
}

impl DaosKeyList {
    pub fn new() -> Box<Self> {
        let vec = vec![0u8; KEY_BUF_SIZE];
        Box::new(DaosKeyList {
            anchor: Box::new(daos_anchor_t {
                da_type: 0,
                da_shard: 0,
                da_flags: 0,
                da_sub_anchors: 0,
                da_buf: [0; DAOS_ANCHOR_BUF_MAX as usize],
            }),
            ndesc: Box::new(MAX_KEY_DESCS),
            key_descs: vec![
                daos_key_desc_t {
                    kd_key_len: 0,
                    kd_val_type: 0,
                };
                MAX_KEY_DESCS as usize
            ],
            out_buf: vec,
        })
    }

    pub fn prepare_next_query(&mut self) {
        *(self.ndesc) = MAX_KEY_DESCS;
    }

    pub fn reach_end(&self) -> bool {
        daos_anchor_is_eof(self.anchor.as_ref())
    }

    // use (0, 0) as start position
    pub fn get_key(&self, start_and_idx: (u32, u32)) -> Result<(&[u8], (u32, u32))> {
        let (start, idx) = start_and_idx;
        if idx >= *self.ndesc {
            return Err(Error::new(ErrorKind::Other, "index out of range"));
        }
        let key_desc = &self.key_descs[idx as usize];
        let end = start as usize + key_desc.kd_key_len as usize;
        let key = &self.out_buf[start as usize..end];
        Ok((key, (end as u32, idx + 1)))
    }
}

pub trait DaosObjSyncOps {
    fn create(
        cont: &DaosContainer,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> Result<Box<DaosObject>>;
    fn open(cont: &DaosContainer, oid: daos_obj_id_t, read_only: bool) -> Result<Box<DaosObject>>;
    fn punch(&self, txn: &DaosTxn) -> Result<()>;
    fn update(
        &self,
        txn: &DaosTxn,
        flags: u64,
        dkey: Vec<u8>,
        akey: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<()>;
}

pub trait DaosObjAsyncOps {
    fn create_async(
        cont: &DaosContainer,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static;
    fn open_async(
        cont: &DaosContainer,
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
    fn list_dkey_async(
        &self,
        txn: &DaosTxn,
        key_lst: Box<DaosKeyList>,
    ) -> impl Future<Output = Result<Box<DaosKeyList>>> + Send + 'static;
}

impl DaosObjSyncOps for DaosObject {
    fn create(
        cont: &DaosContainer,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> Result<Box<DaosObject>> {
        let cont_hdl = cont.get_handle();
        let eq = cont.get_event_queue();
        let eqh = eq.map(|eq| eq.get_handle().unwrap());

        let mut oid = daos_obj_id_t { lo: 0, hi: 0 };
        let ret =
            unsafe { daos_obj_generate_oid2(cont_hdl.unwrap(), &mut oid, otype, cid, hints, args) };

        if ret != 0 {
            return Err(Error::new(ErrorKind::Other, "can't generate object id"));
        }

        let mut obj_hdl = daos_handle_t { cookie: 0u64 };
        let ret = unsafe {
            daos_obj_open(
                cont_hdl.unwrap(),
                oid,
                DAOS_OO_RW,
                &mut obj_hdl,
                std::ptr::null_mut(),
            )
        };

        if ret != 0 {
            return Err(Error::new(ErrorKind::Other, "can't open object"));
        }

        Ok(Box::new(DaosObject::new(oid, obj_hdl, eqh)))
    }

    fn open(
        _cont: &DaosContainer,
        _oid: daos_obj_id_t,
        _read_only: bool,
    ) -> Result<Box<DaosObject>> {
        Err(Error::new(ErrorKind::Other, "Not implemented"))
    }

    fn punch(&self, _txn: &DaosTxn) -> Result<()> {
        Err(Error::new(ErrorKind::Other, "Not implemented"))
    }

    fn update(
        &self,
        txn: &DaosTxn,
        flags: u64,
        dkey: Vec<u8>,
        akey: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<()> {
        let obj_hdl = self.get_handle();
        if obj_hdl.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "update uninitialized object",
            ));
        }

        let txn_hdl = txn.get_handle().unwrap_or(DAOS_TXN_NONE);

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
                txn_hdl,
                flags,
                &mut dkey_wrapper,
                1,
                &mut iod,
                &mut sgl,
                std::ptr::null_mut(),
            )
        };

        if ret != 0 {
            return Err(Error::new(ErrorKind::Other, "Failed to update object"));
        }

        Ok(())
    }
}

impl DaosObjAsyncOps for DaosObject {
    fn create_async(
        cont: &DaosContainer,
        otype: daos_otype_t,
        cid: daos_oclass_id_t,
        hints: daos_oclass_hints_t,
        args: u32,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static {
        let eq = cont.get_event_queue();
        let eqh = eq.map(|eq| eq.get_handle().unwrap());
        let evt = eq.map(|e| e.create_event());
        let cont_hdl = cont.get_handle();
        async move {
            if cont_hdl.is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "empty container handle",
                ));
            }
            if evt.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "event queue is nil"));
            }

            let mut oid = daos_obj_id_t { lo: 0, hi: 0 };
            let ret = unsafe {
                daos_obj_generate_oid2(cont_hdl.unwrap(), &mut oid, otype, cid, hints, args)
            };
            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't generate object id"));
            }

            let mut event = evt.unwrap()?;
            let rx = event.register_callback()?;

            let mut obj_hdl = Box::new(daos_handle_t { cookie: 0u64 });
            let ret = unsafe {
                daos_obj_open(
                    cont_hdl.unwrap(),
                    oid,
                    DAOS_OO_RW,
                    obj_hdl.as_mut(),
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

            Ok(Box::new(DaosObject::new(oid, *obj_hdl, eqh)))
        }
    }

    fn open_async(
        cont: &DaosContainer,
        oid: daos_obj_id_t,
        read_only: bool,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static {
        let eq = cont.get_event_queue();
        let eqh = eq.map(|eq| eq.get_handle().unwrap());
        let evt = eq.map(|e| e.create_event());
        let cont_hdl = cont.get_handle();
        async move {
            if cont_hdl.is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "empty container handle",
                ));
            }
            if evt.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "event queue is nil"));
            }

            let mut event = evt.unwrap()?;
            let rx = event.register_callback()?;

            let mut obj_hdl = Box::new(daos_handle_t { cookie: 0u64 });
            let ret = unsafe {
                daos_obj_open(
                    cont_hdl.unwrap(),
                    oid,
                    if read_only { DAOS_OO_RO } else { DAOS_OO_RW },
                    obj_hdl.as_mut(),
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
                        Ok(Box::new(DaosObject::new(oid, *obj_hdl, eqh)))
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early")),
            }
        }
    }

    fn punch_async(&self, txn: &DaosTxn) -> impl Future<Output = Result<()>> + Send + 'static {
        let eq = self.get_event_queue().clone();
        let obj_hdl = self.get_handle();
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

            let mut event = DaosEvent::new(eq.unwrap())?;
            let rx = event.register_callback()?;

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
        let obj_hdl = self.get_handle();
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

            let mut event = DaosEvent::new(eq.unwrap())?;
            let rx = event.register_callback()?;

            let txn = match tx_hdl {
                Some(tx) => tx,
                None => DAOS_TXN_NONE,
            };

            let mut dkey_wrapper = Box::new(daos_key_t {
                iov_buf: dkey.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                iov_buf_len: dkey.len(),
                iov_len: dkey.len(),
            });
            let mut iod = Box::new(daos_iod_t {
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
            });
            let mut buf = Vec::with_capacity(max_size as usize);
            buf.resize(max_size as usize, 0u8);
            let mut sg_iov = Box::new(d_iov_t {
                iov_buf: buf.as_mut_ptr() as *mut std::os::raw::c_void,
                iov_buf_len: buf.len(),
                iov_len: buf.len(),
            });
            let mut sgl = Box::new(d_sg_list_t {
                sg_nr: 1,
                sg_nr_out: 0,
                sg_iovs: sg_iov.as_mut(),
            });
            let ret = unsafe {
                daos_obj_fetch(
                    obj_hdl.unwrap(),
                    txn,
                    flags,
                    dkey_wrapper.as_mut(),
                    1,
                    iod.as_mut(),
                    sgl.as_mut(),
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
        let obj_hdl = self.get_handle();
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

            let mut event = DaosEvent::new(eq.unwrap())?;
            let rx = event.register_callback()?;

            let txn = match tx_hdl {
                Some(tx) => tx,
                None => DAOS_TXN_NONE,
            };

            let mut dkey_wrapper = Box::new(daos_key_t {
                iov_buf: dkey.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                iov_buf_len: dkey.len(),
                iov_len: dkey.len(),
            });
            let mut iod = Box::new(daos_iod_t {
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
            });
            let mut sg_iov = Box::new(d_iov_t {
                iov_buf: data.as_ptr() as *mut u8 as *mut std::os::raw::c_void,
                iov_buf_len: data.len(),
                iov_len: data.len(),
            });
            let mut sgl = Box::new(d_sg_list_t {
                sg_nr: 1,
                sg_nr_out: 0,
                sg_iovs: sg_iov.as_mut(),
            });
            let ret = unsafe {
                daos_obj_update(
                    obj_hdl.unwrap(),
                    txn,
                    flags,
                    dkey_wrapper.as_mut(),
                    1,
                    iod.as_mut(),
                    sgl.as_mut(),
                    event.as_mut(),
                )
            };
            if ret != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("can't update object, ret={}", ret),
                ));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(
                            ErrorKind::Other,
                            format!("async update operation fail, ret={}", ret),
                        ))
                    } else {
                        Ok(())
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early")),
            }
        }
    }

    fn list_dkey_async(
        &self,
        txn: &DaosTxn,
        key_lst: Box<DaosKeyList>,
    ) -> impl Future<Output = Result<Box<DaosKeyList>>> + Send + 'static {
        let eq = self.get_event_queue().clone();
        let obj_hdl = self.get_handle();
        let tx_hdl = txn.get_handle().clone();
        async move {
            if eq.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "event queue is nil"));
            }
            if obj_hdl.is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "list uninitialized object",
                ));
            }

            let mut key_lst = key_lst;

            let mut event = DaosEvent::new(eq.unwrap())?;
            let rx = event.register_callback()?;

            let txn = match tx_hdl {
                Some(tx) => tx,
                None => DAOS_TXN_NONE,
            };

            key_lst.prepare_next_query();

            let mut sg_iov = Box::new(d_iov_t {
                iov_buf: key_lst.out_buf.as_mut_ptr() as *mut std::os::raw::c_void,
                iov_buf_len: key_lst.out_buf.len(),
                iov_len: key_lst.out_buf.len(),
            });
            let mut sgl = Box::new(d_sg_list_t {
                sg_nr: 1,
                sg_nr_out: 0,
                sg_iovs: sg_iov.as_mut(),
            });

            let res = unsafe {
                daos_obj_list_dkey(
                    obj_hdl.unwrap(),
                    txn,
                    key_lst.ndesc.as_mut(),
                    key_lst.key_descs.as_mut_ptr(),
                    sgl.as_mut(),
                    key_lst.anchor.as_mut(),
                    event.as_mut(),
                )
            };
            if res != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("list dkey fail, err={}", res),
                ));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(
                            ErrorKind::Other,
                            format!("async list dkey fail, ret={}", ret),
                        ))
                    } else {
                        Ok(key_lst)
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "rx is closed early")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bindings::{daos_otype_t_DAOS_OT_MULTI_HASHED, OC_UNKNOWN};
    use crate::daos_pool::DaosPool;

    const TEST_POOL_NAME: &str = "pool1";
    const TEST_CONT_NAME: &str = "cont1";

    #[test]
    fn test_create_sync() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

        let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
        let cid: daos_oclass_id_t = OC_UNKNOWN;
        let hints: daos_oclass_hints_t = 0;
        let args = 0;

        let result = DaosObject::create(&cont, otype, cid, hints, args);

        assert!(result.is_ok());
        let _obj_box = result.unwrap();
        // Assert obj_box is created correctly
    }

    #[test]
    fn test_update_sync() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

        let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
        let cid: daos_oclass_id_t = OC_UNKNOWN;
        let hints: daos_oclass_hints_t = 0;
        let args = 0;

        let result = DaosObject::create(&cont, otype, cid, hints, args);

        assert!(result.is_ok());
        let obj_box = result.unwrap();

        let txn = DaosTxn::txn_none();
        let flags = 0;
        let dkey = vec![0u8, 1u8, 2u8, 3u8];
        let akey = vec![0u8];
        let data = vec![1u8; 256];
        let result = obj_box.update(&txn, flags, dkey, akey, data);
        assert!(result.is_ok());
        // Assert update operation is successful
    }

    #[tokio::test]
    async fn test_create_async() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

        let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
        let cid: daos_oclass_id_t = OC_UNKNOWN;
        let hints: daos_oclass_hints_t = 0;
        let args = 0;

        let result = DaosObject::create_async(&cont, otype, cid, hints, args).await;

        assert!(result.is_ok());
        let _obj_box = result.unwrap();
        // Assert obj_box is created correctly
    }

    #[tokio::test]
    async fn test_open_async() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

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
        let _obj = result.unwrap();
        // Assert obj is opened correctly
    }

    #[tokio::test]
    async fn test_punch_async() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

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
    }

    #[tokio::test]
    async fn test_fetch_async() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

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
        let _data = result.unwrap();
        // Assert fetched data is correct
    }

    #[tokio::test]
    async fn test_update_async() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

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
    }

    #[tokio::test]
    async fn test_list_dkey_async() {
        let mut pool = DaosPool::new(TEST_POOL_NAME);
        pool.connect().expect("Failed to connect to pool");

        let mut cont = DaosContainer::new(TEST_CONT_NAME);
        cont.connect(&pool).expect("Failed to connect to container");

        let otype = daos_otype_t_DAOS_OT_MULTI_HASHED;
        let cid: daos_oclass_id_t = OC_UNKNOWN;
        let hints: daos_oclass_hints_t = 0;
        let args = 0;

        let result = DaosObject::create_async(&cont, otype, cid, hints, args).await;
        assert!(result.is_ok());
        let obj_box = result.unwrap();

        let txn = DaosTxn::txn_none();
        let dkey = "string1".as_bytes().to_vec();
        let akey = vec![0u8];
        let data = vec![1u8; 256];
        let res = obj_box
            .update_async(&txn, DAOS_COND_DKEY_INSERT as u64, dkey, akey, data)
            .await;
        assert!(res.is_ok());

        let dkey = "very_long_string2".as_bytes().to_vec();
        let akey = vec![0u8];
        let data = vec![2u8; 256];
        let res = obj_box
            .update_async(&txn, DAOS_COND_DKEY_INSERT as u64, dkey, akey, data)
            .await;
        assert!(res.is_ok());

        let key_lst = DaosKeyList::new();
        let result = obj_box.list_dkey_async(&txn, key_lst).await;
        assert!(result.is_ok());
        // Assert list dkey operation is successful
        let key_lst = result.unwrap();

        let off = (0u32, 0u32);
        let res = key_lst.get_key(off);
        let off = match res {
            Ok((key, off)) => {
                assert_eq!(key, "string1".as_bytes());
                off
            }
            Err(_) => {
                assert!(false);
                (0u32, 0u32)
            }
        };

        let res = key_lst.get_key(off);
        let off = match res {
            Ok((key, off)) => {
                assert_eq!(key, "very_long_string2".as_bytes());
                off
            }
            Err(_) => {
                assert!(false);
                (0u32, 0u32)
            }
        };

        let res = key_lst.get_key(off);
        assert!(res.is_err());
    }
}