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

use std::io::{Error, ErrorKind, Result};
use std::ptr;
use tokio::sync::oneshot;

use crate::bindings::{daos_event_init, daos_event_register_comp_cb, daos_event_t, daos_handle_t, daos_event__bindgen_ty_1,};

#[derive(Debug)]
pub struct CallbackArg {
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

pub fn create_async_event(
    eq: daos_handle_t,
) -> Result<(Box<daos_event_t>, CallbackArg, oneshot::Receiver<i32>)> {
    let mut event = Box::new(daos_event_t {
        ev_error: 0,
        ev_private: daos_event__bindgen_ty_1 { space: [0u64; 20] },
        ev_debug: 0u64,
    });

    let ret = unsafe { daos_event_init(event.as_mut(), eq, ptr::null_mut()) };
    if ret != 0 {
        return Err(Error::new(ErrorKind::Other, "can't init daos event"));
    }

    let (tx, rx) = oneshot::channel::<i32>();
    let mut call_arg = CallbackArg { tx: Some(tx) };

    let ret = unsafe {
        daos_event_register_comp_cb(
            event.as_mut(),
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

    Ok((event, call_arg, rx))
}
