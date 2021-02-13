/*
 * Copyright 2018 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

#![allow(unknown_lints)]


use std::collections::HashSet;
use std::str;
use std::vec::Vec;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use cpython;
use cpython::ObjectProtocol;
use cpython::PyClone;
use cpython::PyList;
use cpython::Python;

use sawtooth::hashlib::sha256_digest_strs;
use sawtooth::journal::commit_store::CommitStore;
use sawtooth::journal::validation_rule_enforcer;
use sawtooth::state::settings_view::SettingsView;
use sawtooth::{batch::Batch, block::Block, scheduler::Scheduler, transaction::Transaction};

use crate::py_object_wrapper::PyObjectWrapper;
use journal::chain_commit_state::TransactionCommitCache;

use pylogger;

#[derive(Debug)]
pub enum CandidateBlockError {
    BlockEmpty,
}

#[derive(Debug)]
pub struct FinalizeBlockResult {
    pub block: Option<cpython::PyObject>,
    pub remaining_batches: Vec<Batch>,
    pub last_batch: Batch,
    pub injected_batch_ids: Vec<String>,
}

pub struct CandidateBlock {
    previous_block: Block,
    commit_store: CommitStore,
    scheduler: Box<dyn Scheduler>,
    max_batches: usize,
    block_builder: cpython::PyObject,
    batch_injectors: Vec<cpython::PyObject>,
    identity_signer: cpython::PyObject,
    settings_view: SettingsView,

    summary: Option<Vec<u8>>,
    /// Batches remaining after the summary has been computed
    remaining_batches: Vec<Batch>,

    pending_batches: Vec<Batch>,
    pending_batch_ids: HashSet<String>,
    injected_batch_ids: HashSet<String>,

    committed_txn_cache: TransactionCommitCache,
}

impl CandidateBlock {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        previous_block: Block,
        commit_store: CommitStore,
        scheduler: Box<dyn Scheduler>,
        committed_txn_cache: TransactionCommitCache,
        block_builder: cpython::PyObject,
        max_batches: usize,
        batch_injectors: Vec<cpython::PyObject>,
        identity_signer: cpython::PyObject,
        settings_view: SettingsView,
    ) -> Self {
        CandidateBlock {
            previous_block,
            commit_store,
            scheduler,
            max_batches,
            committed_txn_cache,
            block_builder,
            batch_injectors,
            identity_signer,
            settings_view,
            summary: None,
            remaining_batches: vec![],
            pending_batches: vec![],
            pending_batch_ids: HashSet::new(),
            injected_batch_ids: HashSet::new(),
        }
    }

    pub fn cancel(&mut self) {
        self.scheduler.cancel().unwrap();
    }

    pub fn previous_block_id(&self) -> String {
        self.previous_block.header_signature.clone()
    }

    pub fn last_batch(&self) -> Option<&Batch> {
        self.pending_batches.last()
    }

    pub fn can_add_batch(&self) -> bool {
        self.summary.is_none()
            && (self.max_batches == 0 || self.pending_batches.len() < self.max_batches)
    }


    fn check_batch_dependencies_add_batch(&mut self, batch: &Batch) -> bool {
        for txn in &batch.transactions {
            if self.txn_is_already_committed(txn, &self.committed_txn_cache) {
                debug!(
                    "Transaction rejected as it is already in the chain {}",
                    txn.header_signature
                );
                return false;
            } else if !self.check_transaction_dependencies(txn) {
                self.committed_txn_cache.remove_batch(batch);
                return false;
            }
            self.committed_txn_cache.add(txn.header_signature.clone());
        }
        true
    }

    fn check_batch_dependencies(
        &mut self,
        batch: &Batch,
        committed_txn_cache: &mut TransactionCommitCache,
    ) -> bool {
        for txn in &batch.transactions {
            if self.txn_is_already_committed(txn, committed_txn_cache) {
                debug!(
                    "Transaction rejected as it is already in the chain {}",
                    txn.header_signature
                );
                return false;
            } else if !self.check_transaction_dependencies(txn) {
                committed_txn_cache.remove_batch(batch);
                return false;
            }
            committed_txn_cache.add(txn.header_signature.clone());
        }
        true
    }

    fn check_transaction_rewards(&self,  txn: &Transaction) -> bool {
        println!("======= check transation rewards =========");
        // get number of record from chian
        let temp = self.commit_store.get_block_count();
        let total_blocks = temp.unwrap() as u64;
        // unpack blocks, store txn into vector
        let mut transactions: Vec<Vec<u8>> = Vec::new(); 
        let mut x: u64 = 1;
        while x < total_blocks {
            let block = self.commit_store.get_by_block_num(x).unwrap();
            for batch in block.batches {
                for transaction in batch.transactions {
                    transactions.push(transaction.payload);
                }
            }
            x += 1;
        }

        // parse txn string
        let mut txn_data: Vec<String> = Vec::new();
        for t in transactions {
            let txn_str = String::from_utf8_lossy(&t);
            let data:Vec<&str> = txn_str.split(',').collect();
            for v in data {
                txn_data.push(v.to_string());
            }
        }
        println!("========= history transation  ============= {:#?}", txn_data);


        let file_path = Path::new("/home/sawtooth/data.txt");
        let mut file = File::create(&file_path).unwrap();
        println!("========= file  ============= {:#?}", file);
        for value in &txn_data {
            let owned_str: String = value.to_owned();
            let borrowed: &str = ",";
            let new_value = owned_str + borrowed;
            file.write_all(new_value.as_bytes());
        }
        
        // current transaction
        let v = String::from_utf8_lossy(&txn.payload);
        let current_txn:Vec<&str> = v.split(',').collect();
        println!("========= current transation ============= {:#?}", current_txn);
        let publisher = current_txn.get(2).unwrap();
        println!("========= current publisher ============= {:#?}", publisher);
        
        let mut i = 0;
        // history send out rewards
        let mut total_sendout = 0.0;
        // history received rewards
        let mut total_received = 0.0;
        while i < txn_data.len() {
            // as publisher in history
            let p = txn_data.get(i+2).unwrap();
            // as worker in history
            let w = txn_data.get(i+1).unwrap();
            if p == publisher {
                let base: f64 = txn_data.get(i+6).unwrap().parse().unwrap();
                let extra: f64 = txn_data.get(i+7).unwrap().parse().unwrap();
                total_sendout = total_sendout + base + extra;
            }
            if w == publisher {
                let base: f64 = txn_data.get(i+6).unwrap().parse().unwrap();
                let extra: f64 = txn_data.get(i+7).unwrap().parse().unwrap();
                total_received = total_received + base + extra;
            }
            i = i+9;
        }
        println!("========= total send out ============= {}", total_sendout);
        println!("========= total total_received ============= {}", total_received);

        // compute rewards
        let remaining_rewards = total_received-total_sendout;
        println!("========= remaining ============= {}", remaining_rewards);

        let base: f64 = current_txn.get(6).unwrap().parse().unwrap();
        let extra: f64 = current_txn.get(7).unwrap().parse().unwrap();
        let current_sendout = base + extra; 
        println!("========= current send out ============= {}", current_sendout);

        if current_sendout > remaining_rewards {
            return false;
        } else {
            return true;
        }
    }

    fn check_transaction_dependencies(&self, txn: &Transaction) -> bool {
        for dep in &txn.dependencies {
            if !self.committed_txn_cache.contains(dep.as_str()) {
                debug!(
                    "Transaction rejected due to missing dependency, transaction {} depends on {}",
                    txn.header_signature.as_str(),
                    dep.as_str()
                );
                return false;
            }
        }
        true
    }

    fn txn_is_already_committed(
        &self,
        txn: &Transaction,
        committed_txn_cache: &TransactionCommitCache,
    ) -> bool {
        committed_txn_cache.contains(txn.header_signature.as_str())
            || self
                .commit_store
                .contains_transaction(txn.header_signature.as_str())
                .expect("Couldn't check for txn")
    }

    fn batch_is_already_committed(&self, batch: &Batch) -> bool {
        self.pending_batch_ids
            .contains(batch.header_signature.as_str())
            || self
                .commit_store
                .contains_batch(batch.header_signature.as_str())
                .expect("Couldn't check for batch")
    }

    fn poll_injectors<F: Fn(&cpython::PyObject) -> Vec<cpython::PyObject>>(
        &mut self,
        poller: F,
    ) -> Vec<Batch> {
        let mut batches = vec![];
        for injector in &self.batch_injectors {
            let inject_list = poller(injector);
            if !inject_list.is_empty() {
                for b in inject_list {
                    let py_wrapper = PyObjectWrapper::new(b);
                    let batch = Batch::from(py_wrapper);
                    self.injected_batch_ids
                        .insert(batch.header_signature.clone());
                    batches.push(batch);
                }
            }
        }
        batches
    }

    pub fn add_batch(&mut self, batch: Batch) {
        let batch_header_signature = batch.header_signature.clone();
        let mut enough_rewards = true;
        
        if batch.trace {
            debug!(
                "TRACE {}: {}",
                batch_header_signature.as_str(),
                "CandidateBlock, add_batch"
            );
        }

        for txn in &batch.transactions {
            if !self.check_transaction_rewards(txn) {
                enough_rewards = false;
            }
        } 

        if !enough_rewards {
            println!("Publisher does not have enough rewards");
            debug!("Publisher does not have enough rewards");
        } else if self.batch_is_already_committed(&batch) {
            debug!(
                "Dropping previously committed batch: {}",
                batch_header_signature.as_str()
            );
        } else if self.check_batch_dependencies_add_batch(&batch) {
            let mut batches_to_add = vec![];

            // Inject blocks at the beginning of a Candidate Block
            if self.pending_batches.is_empty() {
                let previous_block = self.previous_block.clone();
                let mut injected_batches = self.poll_injectors(|injector: &cpython::PyObject| {
                    let gil = cpython::Python::acquire_gil();
                    let py = gil.python();
                    match injector
                        .call_method(
                            py,
                            "block_start",
                            (PyObjectWrapper::from(previous_block.clone()),),
                            None,
                        )
                        .expect("BlockInjector.block_start failed")
                        .extract::<cpython::PyList>(py)
                    {
                        Ok(injected) => injected.iter(py).collect(),
                        Err(err) => {
                            pylogger::exception(
                                py,
                                "During block injection, calling block_start",
                                err,
                            );
                            vec![]
                        }
                    }
                });
                batches_to_add.append(&mut injected_batches);
            }

            batches_to_add.push(batch);

            {
                let batches_to_test = self
                    .pending_batches
                    .iter()
                    .chain(batches_to_add.iter())
                    .collect::<Vec<_>>();
                if !validation_rule_enforcer::enforce_validation_rules(
                    &self.settings_view,
                    &self.get_signer_public_key_hex(),
                    &batches_to_test,
                ) {
                    return;
                }
            }

            for b in batches_to_add {
                let batch_id = b.header_signature.clone();
                self.pending_batches.push(b.clone());
                self.pending_batch_ids.insert(batch_id.clone());

                let injected = self.injected_batch_ids.contains(batch_id.as_str());

                self.scheduler.add_batch(b, None, injected).unwrap()
            }
        } else {
            debug!(
                "Dropping batch due to missing dependencies: {}",
                batch_header_signature.as_str()
            );
        }
    }

    fn get_signer_public_key_hex(&self) -> String {
        let gil = cpython::Python::acquire_gil();
        let py = gil.python();

        self.identity_signer
            .call_method(py, "get_public_key", cpython::NoArgs, None)
            .expect("IdentitySigner has no method 'get_public_key'")
            .call_method(py, "as_hex", cpython::NoArgs, None)
            .expect("PublicKey has no method 'as_hex'")
            .extract(py)
            .expect("Unable to convert python string to rust")
    }

    pub fn sign_block(&self, block_builder: &cpython::PyObject) {
        let gil = cpython::Python::acquire_gil();
        let py = gil.python();
        let header_bytes = block_builder
            .getattr(py, "block_header")
            .expect("BlockBuilder has no attribute 'block_header'")
            .call_method(py, "SerializeToString", cpython::NoArgs, None)
            .unwrap();
        let signature = self
            .identity_signer
            .call_method(py, "sign", (header_bytes,), None)
            .expect("Signer has no method 'sign'");
        block_builder
            .call_method(py, "set_signature", (signature,), None)
            .expect("BlockBuilder has no method 'set_signature'");
    }

    pub fn summarize(&mut self, force: bool) -> Result<Option<Vec<u8>>, CandidateBlockError> {
        if let Some(ref summary) = self.summary {
            return Ok(Some(summary.clone()));
        }

        if !force && self.pending_batches.is_empty() {
            return Err(CandidateBlockError::BlockEmpty);
        }

        self.scheduler.finalize(true).unwrap();
        let execution_results = self.scheduler.complete(true).unwrap().unwrap();

        let mut committed_txn_cache = TransactionCommitCache::new(self.commit_store.clone());

        let batches_w_no_results: Vec<String> = execution_results
            .batch_results
            .iter()
            .filter(|(_, txns)| txns.is_none())
            .map(|(batch_id, _)| batch_id.clone())
            .collect();

        let valid_batch_ids: HashSet<String> = execution_results
            .batch_results
            .into_iter()
            .filter(|(_, txns)| match txns {
                Some(t) => !t.iter().any(|t| !t.is_valid),
                None => false,
            })
            .map(|(b_id, _)| b_id)
            .collect();

        let builder = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            self.block_builder.clone_ref(py)
        };

        let mut bad_batches = vec![];
        let mut pending_batches = vec![];

        if self.injected_batch_ids == valid_batch_ids {
            // There only injected batches in this block
            return Ok(None);
        }

        for batch in self.pending_batches.clone() {
            let header_signature = &batch.header_signature.clone();
            if batch.trace {
                debug!("TRACE {} : CandidateBlock finalize", header_signature)
            }

            if batches_w_no_results.contains(&batch.header_signature) {
                if !self
                    .injected_batch_ids
                    .contains(batch.header_signature.as_str())
                {
                    pending_batches.push(batch)
                } else {
                    warn! {
                        "Failed to inject batch {}",
                        header_signature
                    };
                }
            } else if valid_batch_ids.contains(&batch.header_signature) {
                if !self.check_batch_dependencies(&batch, &mut committed_txn_cache) {
                    debug!(
                        "Batch {} is invalid, due to missing txn dependency",
                        header_signature
                    );
                    bad_batches.push(batch);
                    pending_batches.clear();
                    pending_batches.append(
                        &mut self
                            .pending_batches
                            .clone()
                            .into_iter()
                            .filter(|b| !bad_batches.contains(b))
                            .collect(),
                    );
                    return Ok(None);
                } else {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    let batch_wrapper = PyObjectWrapper::from(batch.clone());
                    builder
                        .call_method(py, "add_batch", (batch_wrapper,), None)
                        .expect("BlockBuilder has no method 'add_batch'");
                    committed_txn_cache.add_batch(&batch.clone());
                }
            } else {
                bad_batches.push(batch.clone());
                debug!("Batch {} invalid, not added to block", header_signature);
            }
        }
        if execution_results.ending_state_hash.is_none() || self.no_batches_added(&builder) {
            debug!("Abandoning block, no batches added");
            return Ok(None);
        }

        let gil = cpython::Python::acquire_gil();
        let py = gil.python();
        builder
            .call_method(
                py,
                "set_state_hash",
                (execution_results.ending_state_hash,),
                None,
            )
            .expect("BlockBuilder has no method 'set_state_hash'");

        let batch_py_objs = builder
            .getattr(py, "batches")
            .expect("BlockBuilder has no attribute 'batches'")
            .extract::<PyList>(py)
            .expect("Failed to extract PyList from uncommitted_batches")
            .iter(py)
            .map(PyObjectWrapper::new)
            .collect::<Vec<PyObjectWrapper>>();

        let batches = batch_py_objs
            .into_iter()
            .map(Batch::from)
            .collect::<Vec<Batch>>();

        let batch_ids: Vec<&str> = batches
            .iter()
            .map(|batch| batch.header_signature.as_str())
            .collect();

        self.summary = Some(sha256_digest_strs(batch_ids.as_slice()));
        self.remaining_batches = pending_batches;

        Ok(self.summary.clone())
    }

    pub fn finalize(
        &mut self,
        consensus_data: &[u8],
        force: bool,
    ) -> Result<FinalizeBlockResult, CandidateBlockError> {
        let summary = if self.summary.is_none() {
            self.summarize(force)?
        } else {
            self.summary.clone()
        };
        if summary.is_none() {
            return self.build_result(None);
        }

        let builder = &self.block_builder;
        let gil = cpython::Python::acquire_gil();
        let py = gil.python();
        builder
            .getattr(py, "block_header")
            .expect("BlockBuilder has no attribute 'block_header'")
            .setattr(py, "consensus", cpython::PyBytes::new(py, consensus_data))
            .expect("BlockHeader has no attribute 'consensus'");

        self.sign_block(builder);

        self.build_result(Some(
            builder
                .call_method(py, "build_block", cpython::NoArgs, None)
                .expect("BlockBuilder has no method 'build_block'"),
        ))
    }

    fn no_batches_added(&self, builder: &cpython::PyObject) -> bool {
        let gil = cpython::Python::acquire_gil();
        let py = gil.python();
        builder
            .getattr(py, "batches")
            .expect("BlockBuilder has no attribute 'batches'")
            .extract::<cpython::PyList>(py)
            .unwrap()
            .len(py)
            == 0
    }

    fn build_result(
        &self,
        block: Option<cpython::PyObject>,
    ) -> Result<FinalizeBlockResult, CandidateBlockError> {
        if let Some(last_batch) = self.last_batch().cloned() {
            Ok(FinalizeBlockResult {
                block,
                remaining_batches: self.remaining_batches.clone(),
                last_batch,
                injected_batch_ids: self
                    .injected_batch_ids
                    .clone()
                    .into_iter()
                    .collect::<Vec<String>>(),
            })
        } else {
            Err(CandidateBlockError::BlockEmpty)
        }
    }
}
