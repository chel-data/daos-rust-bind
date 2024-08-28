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
use tokio::sync::oneshot;

use crate::bindings::{
    daos_event__bindgen_ty_1, daos_event_init, daos_event_register_comp_cb, daos_event_t,
    daos_handle_t, daos_obj_generate_oid2, daos_obj_id_t, daos_obj_open, daos_oclass_hints_t,
    daos_oclass_id_t, daos_otype_t, DAOS_OO_RO, DAOS_OO_RW,
};
use crate::daos::{DaosContainer, DaosObject};

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
    fn punch_async(&self) -> impl Future<Output = Result<()>> + Send + 'static;
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

struct CallbackArg {
    tx: Option<oneshot::Sender<i32>>,
}

unsafe extern "C" fn event_callback(
    arg1: *mut ::std::os::raw::c_void,
    arg2: *mut daos_event_t,
    arg3: ::std::os::raw::c_int,
) -> i32 {
    let call_arg = arg1 as *mut CallbackArg;
    let sender = (*call_arg).tx.take();
    match sender {
        Some(tx) => {
            if let Err(_) = tx.send(arg3) {
                eprintln!("Failed to send event callback result");
                -1
            } else {
                0
            }
        }
        None => -1,
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
        let mut event = Box::new(daos_event_t {
            ev_error: 0,
            ev_private: daos_event__bindgen_ty_1 { space: [0u64; 20] },
            ev_debug: 0u64,
        });
        let eq = cont.get_event_queue();
        let cont_hdl = cont.get_handle();
        async move {
            let ret = unsafe {
                daos_event_init(event.as_mut() as *mut daos_event_t, eq, ptr::null_mut())
            };
            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't init daos event"));
            }

            let mut oid = daos_obj_id_t { lo: 0, hi: 0 };
            let ret =
                unsafe { daos_obj_generate_oid2(cont_hdl, &mut oid, otype, cid, hints, args) };

            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't generate object id"));
            }

            let (tx, rx) = oneshot::channel::<i32>();
            let mut call_arg: CallbackArg = CallbackArg { tx: Some(tx) };

            let ret = unsafe {
                daos_event_register_comp_cb(
                    event.as_mut() as *mut daos_event_t,
                    Some(event_callback),
                    &mut call_arg as *mut CallbackArg as *mut ::std::os::raw::c_void,
                )
            };
            if ret != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    "can't register event callback",
                ));
            }

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
                        return Err(Error::new(
                            ErrorKind::Other,
                            "get async open object failure",
                        ));
                    }
                }
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::ConnectionReset,
                        "rx is closed early",
                    ));
                }
            }

            Ok(Box::new(DaosObject::new(oid, obj_hdl)))
        }
    }

    fn open_async(
        cont: &DaosContainer<'_>,
        oid: daos_obj_id_t,
        read_only: bool,
    ) -> impl Future<Output = Result<Box<DaosObject>>> + Send + 'static {
        let mut event = Box::new(daos_event_t {
            ev_error: 0,
            ev_private: daos_event__bindgen_ty_1 { space: [0u64; 20] },
            ev_debug: 0u64,
        });
        let eq = cont.get_event_queue();
        let cont_hdl = cont.get_handle();
        async move {
            let ret = unsafe {
                daos_event_init(event.as_mut() as *mut daos_event_t, eq, ptr::null_mut())
            };
            if ret != 0 {
                return Err(Error::new(ErrorKind::Other, "can't init daos event"));
            }

            let (tx, rx) = oneshot::channel::<i32>();
            let mut call_arg: CallbackArg = CallbackArg { tx: Some(tx) };

            let ret = unsafe {
                daos_event_register_comp_cb(
                    event.as_mut() as *mut daos_event_t,
                    Some(event_callback),
                    &mut call_arg as *mut CallbackArg as *mut ::std::os::raw::c_void,
                )
            };
            if ret != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    "can't register event callback",
                ));
            }

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
                        return Err(Error::new(
                            ErrorKind::Other,
                            "get async open object failure",
                        ));
                    }
                }
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::ConnectionReset,
                        "rx is closed early",
                    ));
                }
            }

            Ok(Box::new(DaosObject::new(oid, obj_hdl)))
        }
    }

    fn punch_async(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        async { Err(Error::new(ErrorKind::Other, "Not implemented")) }
    }
}
