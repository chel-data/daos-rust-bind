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

use crate::daos_event::DaosEvent;
use crate::bindings::{
    daos_event_t, daos_handle_t, daos_tx_abort, daos_tx_close, daos_tx_commit, daos_tx_open,
};
use crate::daos_cont::DaosContainer;
use std::future::Future;
use std::{
    io::{Error, ErrorKind, Result},
    option::Option,
};

pub struct DaosTxn {
    handle: Option<daos_handle_t>,
    event_que: Option<daos_handle_t>,
}

impl DaosTxn {
    pub fn txn_none() -> Self {
        DaosTxn {
            handle: None,
            event_que: None,
        }
    }
    pub fn get_handle(&self) -> Option<daos_handle_t> {
        self.handle.clone()
    }
}

pub trait DaosTxnSyncOps {
    fn open(cont: &DaosContainer, flags: u64) -> Result<Box<DaosTxn>>;
    fn commit(&self) -> Result<()>;
    fn abort(&self) -> Result<()>;
    fn close(&self) -> Result<()>;
}

pub trait DaosTxnAsyncOps {
    fn open_async(
        cont: &DaosContainer,
        flags: u64,
    ) -> impl Future<Output = Result<Box<DaosTxn>>> + Send + 'static;
    fn commit_async(&self) -> impl Future<Output = Result<()>> + Send + 'static;
    fn abort_async(&self) -> impl Future<Output = Result<()>> + Send + 'static;
    fn close_async(&self) -> impl Future<Output = Result<()>> + Send + 'static;
}

impl DaosTxnAsyncOps for DaosTxn {
    fn open_async(
        cont: &DaosContainer,
        flags: u64,
    ) -> impl Future<Output = Result<Box<DaosTxn>>> + Send + 'static {
        let cont_hdl = cont.get_handle();
        let eq = cont.get_event_queue();
        let eqh = eq.map(|e| e.get_handle().unwrap());
        let evt = eq.map(|e| e.create_event());
        async move {
            if cont_hdl.is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "empty container handle",
                ));
            }
            if evt.is_none() {
                return Err(Error::new(ErrorKind::InvalidInput, "empty event queue"));
            }
            let res = evt.unwrap();
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let mut event = res.unwrap();

            let res = event.register_callback();
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let rx = res.unwrap();

            let mut tx_hdl = daos_handle_t { cookie: 0u64 };
            let res = unsafe {
                daos_tx_open(
                    cont_hdl.unwrap(),
                    &mut tx_hdl,
                    flags,
                    event.as_mut() as *mut daos_event_t,
                )
            };
            if res != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    "fail to open DAOS transaction",
                ));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(
                            ErrorKind::Other,
                            "async open txn request failed",
                        ))
                    } else {
                        Ok(Box::new(DaosTxn {
                            handle: Some(tx_hdl),
                            event_que: eqh,
                        }))
                    }
                }
                Err(_) => Err(Error::new(
                    ErrorKind::Other,
                    "can't get response from the receiver end",
                )),
            }
        }
    }

    fn commit_async(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        let txn_hdl = self.handle;
        let eq: Option<_> = self.event_que.clone();
        async move {
            if txn_hdl.is_none() || eq.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "commit empty txn"));
            }

            let res = DaosEvent::new(eq.unwrap());
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let mut event = res.unwrap();

            let res = event.register_callback();
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let rx = res.unwrap();

            let res = unsafe { daos_tx_commit(txn_hdl.unwrap(), event.as_mut()) };
            if res != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Failed to commit DAOS transaction",
                ));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(ErrorKind::Other, "txn async commit failed"))
                    } else {
                        Ok(())
                    }
                }
                Err(_) => Err(Error::new(
                    ErrorKind::Other,
                    "txn async commit receiver error",
                )),
            }
        }
    }

    fn abort_async(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        let tx_hdl = self.get_handle();
        let eq = self.event_que.clone();
        async move {
            if tx_hdl.is_none() || eq.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "abort empty txn"));
            }

            let res = DaosEvent::new(eq.unwrap());
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let mut event = res.unwrap();

            let res = event.register_callback();
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let rx = res.unwrap();

            let res = unsafe { daos_tx_abort(tx_hdl.unwrap(), event.as_mut()) };
            if res != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Failed to abort DAOS transaction",
                ));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(ErrorKind::Other, "txn async abort failed"))
                    } else {
                        Ok(())
                    }
                }
                Err(_) => Err(Error::new(
                    ErrorKind::Other,
                    "txn async abort receiver error",
                )),
            }
        }
    }

    fn close_async(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        let tx_hdl = self.get_handle();
        let eq = self.event_que.clone();
        async move {
            if tx_hdl.is_none() || eq.is_none() {
                return Err(Error::new(ErrorKind::InvalidData, "close empty txn"));
            }

            let res = DaosEvent::new(eq.unwrap());
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let mut event = res.unwrap();

            let res = event.register_callback();
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            let rx = res.unwrap();

            let res = unsafe { daos_tx_close(tx_hdl.unwrap(), event.as_mut()) };
            if res != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Failed to close DAOS transaction",
                ));
            }

            match rx.await {
                Ok(ret) => {
                    if ret != 0 {
                        Err(Error::new(ErrorKind::Other, "txn async close failed"))
                    } else {
                        Ok(())
                    }
                }
                Err(_) => Err(Error::new(
                    ErrorKind::Other,
                    "txn async close receiver error",
                )),
            }
        }
    }
}
