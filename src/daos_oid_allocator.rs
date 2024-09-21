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

use crate::bindings::OID_FMT_INTR_BITS;
use crate::daos_cont::{DaosContainer, DaosContainerSyncOps};
use crate::daos_obj::{
    DaosObjAsyncOps, DaosObjSyncOps, DaosObject, DAOS_COND_DKEY_FETCH, DAOS_COND_DKEY_INSERT,
    DAOS_COND_DKEY_UPDATE,
};
use crate::daos_pool::DaosObjectId;
use crate::daos_txn::{DaosTxn, DaosTxnAsyncOps, DaosTxnSyncOps};
use std::io::{Error, ErrorKind, Result};
use std::ops::Range;
use std::sync::Arc;

const OID_BATCH_SIZE: u128 = 1u128 << 10;
const OID_BATCH_CURSOR_KEY: &str = "OID_BATCH_CURSOR";
const OID_BATCH_CURSOR_START: u128 = 1024;

pub struct DaosAsyncOidAllocator {
    range: tokio::sync::Mutex<Range<u128>>,
    cont: Arc<DaosContainer>,
    meta_obj: Box<DaosObject>,
}

pub struct DaosSyncOidAllocator {
    range: std::sync::Mutex<Range<u128>>,
    cont: Arc<DaosContainer>,
    meta_obj: Box<DaosObject>,
}

impl DaosAsyncOidAllocator {
    pub fn new(cont: Arc<DaosContainer>) -> Result<Box<Self>> {
        let prop = cont.query_prop()?;
        let co_roots = prop.get_co_roots()?;
        let meta_oid = co_roots[0];

        let obj = DaosObject::open(cont.as_ref(), meta_oid, false)?;

        Ok(Box::new(DaosAsyncOidAllocator {
            range: tokio::sync::Mutex::new(0..0),
            cont: cont,
            meta_obj: obj,
        }))
    }

    pub async fn allocate(&self) -> Result<DaosObjectId> {
        let mut range = self.range.lock().await;
        if range.start >= range.end {
            drop(range);
            let new_range = self.allocate_oid_batch().await?;
            let mut range = self.range.lock().await;
            *range = new_range;
            if (range.start >> (128 - OID_FMT_INTR_BITS)) != 0 {
                Err(Error::new(ErrorKind::Other, "No more OIDs available"))
            } else {
                let hi = range.start >> 64;
                let lo = range.start & 0xFFFF_FFFF_FFFF_FFFF;
                range.start += 1;
                Ok(DaosObjectId {
                    hi: hi as u64,
                    lo: lo as u64,
                })
            }
        } else {
            if (range.start >> (128 - OID_FMT_INTR_BITS)) != 0 {
                Err(Error::new(ErrorKind::Other, "No more OIDs available"))
            } else {
                let hi = range.start >> 64;
                let lo = range.start & 0xFFFF_FFFF_FFFF_FFFF;
                range.start += 1;
                Ok(DaosObjectId {
                    hi: hi as u64,
                    lo: lo as u64,
                })
            }
        }
    }

    async fn allocate_oid_batch(&self) -> Result<Range<u128>> {
        let txn = DaosTxn::open_async(self.cont.as_ref(), 0).await?;

        let dkey = OID_BATCH_CURSOR_KEY.as_bytes().to_vec();
        let akey = vec![0u8];
        let res = self
            .meta_obj
            .fetch_async(
                &txn,
                DAOS_COND_DKEY_FETCH as u64,
                dkey.clone(),
                akey.clone(),
                32,
            )
            .await;

        let query_again = if res.is_err() {
            let initial = OID_BATCH_CURSOR_START + OID_BATCH_SIZE;
            let data = initial.to_le_bytes().to_vec();

            let res = self
                .meta_obj
                .update_async(
                    &DaosTxn::txn_none(),
                    DAOS_COND_DKEY_INSERT as u64,
                    dkey.clone(),
                    akey.clone(),
                    data,
                )
                .await;
            if res.is_err() {
                true
            } else {
                return Ok(Range {
                    start: OID_BATCH_CURSOR_START,
                    end: OID_BATCH_CURSOR_START + OID_BATCH_SIZE,
                });
            }
        } else {
            false
        };

        let (txn, range_start) = if query_again {
            drop(txn);
            let txn = DaosTxn::open_async(self.cont.as_ref(), 0).await?;
            let res = self
                .meta_obj
                .fetch_async(
                    txn.as_ref(),
                    DAOS_COND_DKEY_FETCH as u64,
                    dkey.clone(),
                    akey.clone(),
                    32,
                )
                .await?;
            (txn, u128::from_le_bytes(res.try_into().unwrap()))
        } else {
            (txn, u128::from_le_bytes(res.unwrap().try_into().unwrap()))
        };

        self.meta_obj
            .update_async(
                txn.as_ref(),
                DAOS_COND_DKEY_UPDATE as u64,
                dkey.clone(),
                akey.clone(),
                (range_start + OID_BATCH_SIZE).to_le_bytes().to_vec(),
            )
            .await?;

        txn.commit_async().await?;

        Ok(Range {
            start: range_start,
            end: range_start + OID_BATCH_SIZE,
        })
    }


}

impl DaosSyncOidAllocator {
    pub fn new(cont: Arc<DaosContainer>) -> Result<Box<Self>> {
        let prop = cont.query_prop()?;
        let co_roots = prop.get_co_roots()?;
        let meta_oid = co_roots[0];

        let obj = DaosObject::open(cont.as_ref(), meta_oid, false)?;

        Ok(Box::new(DaosSyncOidAllocator {
            range: std::sync::Mutex::new(0..0),
            cont: cont,
            meta_obj: obj,
        }))
    }

    pub fn allocate(&self) -> Result<DaosObjectId> {
        let range = self.range.lock().unwrap();
        let mut range = if range.start >= range.end {
            drop(range);
            let new_range = self.allocate_oid_batch()?;
            let mut range = self.range.lock().unwrap();
            *range = new_range;
            range
        } else {
            range
        };

        if (range.start >> (128 - OID_FMT_INTR_BITS)) != 0 {
            Err(Error::new(ErrorKind::Other, "No more OIDs available"))
        } else {
            let hi = range.start >> 64;
            let lo = range.start & 0xFFFF_FFFF_FFFF_FFFF;
            range.start += 1;
            Ok(DaosObjectId {
                hi: hi as u64,
                lo: lo as u64,
            })
        }
    }

    fn allocate_oid_batch(&self) -> Result<Range<u128>> {
        let txn = DaosTxn::open(self.cont.as_ref(), 0)?;

        let dkey = OID_BATCH_CURSOR_KEY.as_bytes().to_vec();
        let akey = vec![0u8];
        let res = self
            .meta_obj
            .fetch(
                &txn,
                DAOS_COND_DKEY_FETCH as u64,
                dkey.clone(),
                akey.clone(),
                32,
            );

        let query_again = if res.is_err() {
            let initial = OID_BATCH_CURSOR_START + OID_BATCH_SIZE;
            let data = initial.to_le_bytes().to_vec();

            let res = self
                .meta_obj
                .update(
                    &DaosTxn::txn_none(),
                    DAOS_COND_DKEY_INSERT as u64,
                    dkey.clone(),
                    akey.clone(),
                    data,
                );
            if res.is_err() {
                true
            } else {
                return Ok(Range {
                    start: OID_BATCH_CURSOR_START,
                    end: OID_BATCH_CURSOR_START + OID_BATCH_SIZE,
                });
            }
        } else {
            false
        };

        let (txn, range_start) = if query_again {
            drop(txn);
            let txn = DaosTxn::open(self.cont.as_ref(), 0)?;
            let res = self
                .meta_obj
                .fetch(
                    txn.as_ref(),
                    DAOS_COND_DKEY_FETCH as u64,
                    dkey.clone(),
                    akey.clone(),
                    32,
                )?;
            (txn, u128::from_le_bytes(res.try_into().unwrap()))
        } else {
            (txn, u128::from_le_bytes(res.unwrap().try_into().unwrap()))
        };

        self.meta_obj
            .update(
                txn.as_ref(),
                DAOS_COND_DKEY_UPDATE as u64,
                dkey.clone(),
                akey.clone(),
                (range_start + OID_BATCH_SIZE).to_le_bytes().to_vec(),
            )?;

        txn.commit()?;

        Ok(Range {
            start: range_start,
            end: range_start + OID_BATCH_SIZE,
        })
    }
}
