// core/src/rdma_context.rs
//
// Ported to ibverbs-sys 0.3 (the raw FFI crate used by ibverbs 0.9.2).

use ibverbs_sys::*;
use std::ptr;

const RDMA_PORT: u8 = 1;
const DEFAULT_GID_INDEX: i32 = 0;

/// Everything RDMA for one endpoint (server or client).
pub struct RdmaContext {
    pub ctx: *mut ibv_context,
    pub pd:  *mut ibv_pd,
    pub mr:  *mut ibv_mr,
    pub cq:  *mut ibv_cq,
    pub qp:  *mut ibv_qp,

    pub buf:     *mut u8,
    pub buf_len: usize,

    pub qpn:  u32,
    pub lid:  u16,
    pub gid:  ibv_gid,
    pub rkey: u32,
    pub gid_index: u8,

    /// Tracks unsignaled WRITE WRs in-flight.
    /// Auto-drains every 64 posts to prevent SQ overflow.
    unsignaled_count: std::cell::Cell<u32>,
}

unsafe impl Send for RdmaContext {}
unsafe impl Sync for RdmaContext {}

fn gid_index() -> i32 {
    if let Some(idx) = std::env::var("RDMA_GID_INDEX")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return idx;
    }

    auto_gid_index().unwrap_or(DEFAULT_GID_INDEX)
}

fn auto_gid_index() -> Option<i32> {
    let devices = std::fs::read_dir("/sys/class/infiniband").ok()?;
    let mut first_non_zero = None;

    for device in devices.flatten() {
        let gids_dir = device.path().join(format!("ports/{RDMA_PORT}/gids"));
        let gids = match std::fs::read_dir(gids_dir) {
            Ok(gids) => gids,
            Err(_) => continue,
        };

        for gid_file in gids.flatten() {
            let idx = match gid_file.file_name().to_string_lossy().parse::<i32>() {
                Ok(idx) => idx,
                Err(_) => continue,
            };
            let gid = match std::fs::read_to_string(gid_file.path()) {
                Ok(gid) => gid.trim().to_ascii_lowercase(),
                Err(_) => continue,
            };

            if gid == "0000:0000:0000:0000:0000:0000:0000:0000" {
                continue;
            }
            if first_non_zero.is_none() {
                first_non_zero = Some(idx);
            }
            if gid.starts_with("0000:0000:0000:0000:0000:ffff:") {
                return Some(idx);
            }
        }
    }

    first_non_zero
}

impl RdmaContext {
    pub fn new(buf_len: usize) -> Self {
        unsafe {
            let mut num_devices: i32 = 0;
            let dev_list = ibv_get_device_list(&mut num_devices);
            assert!(!dev_list.is_null() && num_devices > 0,
                    "No RDMA devices found. Is SoftRoCE / a HCA loaded?");

            let ctx = ibv_open_device(*dev_list);
            ibv_free_device_list(dev_list);
            assert!(!ctx.is_null(), "ibv_open_device failed");

            let pd = ibv_alloc_pd(ctx);
            assert!(!pd.is_null(), "ibv_alloc_pd failed");

            let buf = std::alloc::alloc_zeroed(
                std::alloc::Layout::from_size_align(buf_len, 128).unwrap(),
            );
            assert!(!buf.is_null(), "alloc_zeroed failed");

            let access = (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE).0 as i32;

            let mr = ibv_reg_mr(pd, buf as *mut _, buf_len, access);
            assert!(!mr.is_null(), "ibv_reg_mr failed");

            let cq = ibv_create_cq(ctx, 128, ptr::null_mut(), ptr::null_mut(), 0);
            assert!(!cq.is_null(), "ibv_create_cq failed");

            let mut qp_init: ibv_qp_init_attr = std::mem::zeroed();
            qp_init.send_cq = cq;
            qp_init.recv_cq = cq;
            qp_init.qp_type = ibv_qp_type::IBV_QPT_RC;
            qp_init.cap.max_send_wr     = 128;
            qp_init.cap.max_recv_wr     = 128;
            qp_init.cap.max_send_sge    = 1;
            qp_init.cap.max_recv_sge    = 1;
            qp_init.cap.max_inline_data = 64;

            let qp = ibv_create_qp(pd, &mut qp_init);
            assert!(!qp.is_null(), "ibv_create_qp failed");

            let mut port_attr: ibv_port_attr = std::mem::zeroed();
            let rc = ibv_query_port(
                ctx, RDMA_PORT,
                &mut port_attr as *mut ibv_port_attr as *mut _compat_ibv_port_attr,
            );
            assert_eq!(rc, 0, "ibv_query_port failed");

            // Query and later connect with the same GID index.  Using a GID
            // index that is absent or from a different RoCE address family
            // makes the RTR transition fail with EINVAL on many setups.
            let gid_index = gid_index();
            let mut gid: ibv_gid = std::mem::zeroed();
            let rc = ibv_query_gid(ctx, RDMA_PORT, gid_index, &mut gid);
            assert_eq!(rc, 0, "ibv_query_gid failed");

            let qpn  = (*qp).qp_num;
            let lid  = port_attr.lid;
            let rkey = (*mr).rkey;

            Self {
                ctx, pd, mr, cq, qp,
                buf, buf_len,
                qpn, lid, gid, rkey,
                gid_index: gid_index as u8,
                unsignaled_count: std::cell::Cell::new(0),
            }
        }
    }

    /// Create an RdmaContext that registers a **caller-supplied** buffer.
    ///
    /// Used by the server so multiple QPs can each register the same
    /// physical table memory with distinct rkeys.  The buffer is NOT freed
    /// on drop — the caller owns it.
    ///
    /// # Safety
    /// `buf` must remain valid and unmoved for the lifetime of this context.
    pub fn new_with_buf(buf: *mut u8, buf_len: usize) -> Self {
        unsafe {
            let mut num_devices: i32 = 0;
            let dev_list = ibv_get_device_list(&mut num_devices);
            assert!(!dev_list.is_null() && num_devices > 0, "No RDMA devices");

            let ctx = ibv_open_device(*dev_list);
            ibv_free_device_list(dev_list);
            assert!(!ctx.is_null(), "ibv_open_device failed");

            let pd = ibv_alloc_pd(ctx);
            assert!(!pd.is_null(), "ibv_alloc_pd failed");

            let access = (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE).0 as i32;

            let mr = ibv_reg_mr(pd, buf as *mut _, buf_len, access);
            assert!(!mr.is_null(), "ibv_reg_mr failed");

            let cq = ibv_create_cq(ctx, 128, ptr::null_mut(), ptr::null_mut(), 0);
            assert!(!cq.is_null(), "ibv_create_cq failed");

            let mut qp_init: ibv_qp_init_attr = std::mem::zeroed();
            qp_init.send_cq = cq;
            qp_init.recv_cq = cq;
            qp_init.qp_type = ibv_qp_type::IBV_QPT_RC;
            qp_init.cap.max_send_wr     = 128;
            qp_init.cap.max_recv_wr     = 128;
            qp_init.cap.max_send_sge    = 1;
            qp_init.cap.max_recv_sge    = 1;
            qp_init.cap.max_inline_data = 64;

            let qp = ibv_create_qp(pd, &mut qp_init);
            assert!(!qp.is_null(), "ibv_create_qp failed");

            let mut port_attr: ibv_port_attr = std::mem::zeroed();
            let rc = ibv_query_port(
                ctx, RDMA_PORT,
                &mut port_attr as *mut ibv_port_attr as *mut _compat_ibv_port_attr,
            );
            assert_eq!(rc, 0, "ibv_query_port failed");

            let gid_index = gid_index();
            let mut gid: ibv_gid = std::mem::zeroed();
            let rc = ibv_query_gid(ctx, RDMA_PORT, gid_index, &mut gid);
            assert_eq!(rc, 0, "ibv_query_gid failed");

            let qpn  = (*qp).qp_num;
            let lid  = port_attr.lid;
            let rkey = (*mr).rkey;

            Self {
                ctx, pd, mr, cq, qp,
                buf,       // caller-owned; Drop will NOT free it (buf_len = 0 sentinel)
                buf_len: 0, // sentinel: Drop skips dealloc when buf_len == 0
                qpn, lid, gid, rkey,
                gid_index: gid_index as u8,
                unsignaled_count: std::cell::Cell::new(0),
            }
        }
    }

    /// Transition QP RESET → INIT.
    pub fn move_to_init(&self) {
        unsafe {
            let mut attr: ibv_qp_attr = std::mem::zeroed();
            attr.qp_state        = ibv_qp_state::IBV_QPS_INIT;
            attr.pkey_index      = 0;
            attr.port_num        = RDMA_PORT;
            attr.qp_access_flags = (ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE).0;

            let mask = (ibv_qp_attr_mask::IBV_QP_STATE
                | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
                | ibv_qp_attr_mask::IBV_QP_PORT
                | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS).0 as i32;

            let rc = ibv_modify_qp(self.qp, &mut attr, mask);
            assert_eq!(rc, 0, "INIT transition failed");
        }
    }

    /// Transition QP INIT → RTR.
    pub fn connect_rtr(&self, remote_qpn: u32, remote_lid: u16, remote_gid: ibv_gid) {
        unsafe {
            let mut attr: ibv_qp_attr = std::mem::zeroed();
            attr.qp_state           = ibv_qp_state::IBV_QPS_RTR;
            attr.path_mtu           = IBV_MTU_1024 ;
            attr.dest_qp_num        = remote_qpn;
            attr.rq_psn             = 0;
            attr.max_dest_rd_atomic = 1;
            attr.min_rnr_timer      = 12;

            attr.ah_attr.dlid          = remote_lid;
            attr.ah_attr.sl            = 0;
            attr.ah_attr.src_path_bits = 0;
            attr.ah_attr.port_num      = RDMA_PORT;

            attr.ah_attr.is_global         = 1;
            attr.ah_attr.grh.dgid          = remote_gid;
            attr.ah_attr.grh.sgid_index    = self.gid_index;
            attr.ah_attr.grh.hop_limit     = 64;
            attr.ah_attr.grh.traffic_class = 0;
            attr.ah_attr.grh.flow_label    = 0;

            let mask = (ibv_qp_attr_mask::IBV_QP_STATE
                | ibv_qp_attr_mask::IBV_QP_AV
                | ibv_qp_attr_mask::IBV_QP_PATH_MTU
                | ibv_qp_attr_mask::IBV_QP_DEST_QPN
                | ibv_qp_attr_mask::IBV_QP_RQ_PSN
                | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
                | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER).0 as i32;

            let rc = ibv_modify_qp(self.qp, &mut attr, mask);
            assert_eq!(
                rc, 0,
                "RTR transition failed: rc={rc}, local_qpn={}, remote_qpn={remote_qpn}, remote_lid={remote_lid}, gid_index={}, port={RDMA_PORT}",
                self.qpn,
                self.gid_index,
            );
        }
    }

    /// Transition QP RTR → RTS.
    pub fn connect_rts(&self) {
        unsafe {
            let mut attr: ibv_qp_attr = std::mem::zeroed();
            attr.qp_state      = ibv_qp_state::IBV_QPS_RTS;
            attr.timeout       = 14;
            attr.retry_cnt     = 7;
            attr.rnr_retry     = 7;
            attr.sq_psn        = 0;
            attr.max_rd_atomic = 1;

            let mask = (ibv_qp_attr_mask::IBV_QP_STATE
                | ibv_qp_attr_mask::IBV_QP_TIMEOUT
                | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
                | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
                | ibv_qp_attr_mask::IBV_QP_SQ_PSN
                | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC).0 as i32;

            let rc = ibv_modify_qp(self.qp, &mut attr, mask);
            assert_eq!(rc, 0, "RTS transition failed");
        }
    }

    /// Post one RDMA READ (SIGNALED).
    pub unsafe fn post_read(
        &self,
        wr_id:        u64,
        local_offset: usize,
        len:          u32,
        remote_addr:  u64,
        remote_rkey:  u32,
    ) {
        let mut sge: ibv_sge = std::mem::zeroed();
        sge.addr   = self.buf as u64 + local_offset as u64;
        sge.length = len;
        sge.lkey   = (*self.mr).lkey;

        let mut wr: ibv_send_wr = std::mem::zeroed();
        wr.wr_id      = wr_id;
        wr.opcode     = ibv_wr_opcode::IBV_WR_RDMA_READ;
        wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        wr.sg_list    = &mut sge;
        wr.num_sge    = 1;
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey        = remote_rkey;

        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
        let rc = (*(*self.qp).context).ops
            .post_send.expect("post_send null")(self.qp, &mut wr, &mut bad_wr);
        assert_eq!(rc, 0, "ibv_post_send (READ) failed");
    }

    /// Post one RDMA READ (UNSIGNALED).
    pub unsafe fn post_read_unsignaled(
        &self,
        wr_id:        u64,
        local_offset: usize,
        len:          u32,
        remote_addr:  u64,
        remote_rkey:  u32,
    ) {
        let mut sge: ibv_sge = std::mem::zeroed();
        sge.addr   = self.buf as u64 + local_offset as u64;
        sge.length = len;
        sge.lkey   = (*self.mr).lkey;

        let mut wr: ibv_send_wr = std::mem::zeroed();
        wr.wr_id      = wr_id;
        wr.opcode     = ibv_wr_opcode::IBV_WR_RDMA_READ;
        wr.send_flags = 0;
        wr.sg_list    = &mut sge;
        wr.num_sge    = 1;
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey        = remote_rkey;

        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
        let rc = (*(*self.qp).context).ops
            .post_send.expect("post_send null")(self.qp, &mut wr, &mut bad_wr);
        assert_eq!(rc, 0, "ibv_post_send (UNSIGNALED READ) failed");
    }

    /// Post N logical GETs as one linked ibv_post_send chain.
    ///
    /// Each logical GET is two RDMA READ WRs:
    /// - table A into local offset `i * local_stride`
    /// - table B into local offset `i * local_stride + 64`
    ///
    /// Only the B read is signaled, so callers should poll exactly N
    /// completions.  The signaled WR id is the key index `i`.
    pub unsafe fn post_read_pairs_batched(
        &self,
        remote_addrs: &[(u64, u64)],
        local_stride: usize,
        len: u32,
        remote_rkey: u32,
    ) {
        assert!(!remote_addrs.is_empty(), "empty RDMA READ batch");
        assert!(
            remote_addrs.len() * 2 <= 128,
            "batch has {} WRs, but max_send_wr is 128",
            remote_addrs.len() * 2,
        );
        assert!(
            remote_addrs.len() * local_stride <= self.buf_len,
            "local batch buffer too small",
        );

        let total = remote_addrs.len() * 2;
        let mut sges: Vec<ibv_sge> = vec![std::mem::zeroed(); total];
        let mut wrs: Vec<ibv_send_wr> = vec![std::mem::zeroed(); total];

        for (i, (addr_a, addr_b)) in remote_addrs.iter().copied().enumerate() {
            let a = i * 2;
            let b = a + 1;
            let base = i * local_stride;

            sges[a].addr = self.buf as u64 + base as u64;
            sges[a].length = len;
            sges[a].lkey = (*self.mr).lkey;

            sges[b].addr = self.buf as u64 + (base + len as usize) as u64;
            sges[b].length = len;
            sges[b].lkey = (*self.mr).lkey;

            wrs[a].wr_id = i as u64;
            wrs[a].opcode = ibv_wr_opcode::IBV_WR_RDMA_READ;
            wrs[a].send_flags = 0;
            wrs[a].sg_list = &mut sges[a];
            wrs[a].num_sge = 1;
            wrs[a].wr.rdma.remote_addr = addr_a;
            wrs[a].wr.rdma.rkey = remote_rkey;

            wrs[b].wr_id = i as u64;
            wrs[b].opcode = ibv_wr_opcode::IBV_WR_RDMA_READ;
            wrs[b].send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
            wrs[b].sg_list = &mut sges[b];
            wrs[b].num_sge = 1;
            wrs[b].wr.rdma.remote_addr = addr_b;
            wrs[b].wr.rdma.rkey = remote_rkey;
        }

        for i in 0..total - 1 {
            wrs[i].next = wrs.as_mut_ptr().add(i + 1);
        }

        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
        let rc = (*(*self.qp).context).ops
            .post_send.expect("post_send null")(self.qp, wrs.as_mut_ptr(), &mut bad_wr);
        assert_eq!(rc, 0, "ibv_post_send (BATCHED READ) failed");
    }

    /// Post one RDMA WRITE (UNSIGNALED) — HERD-style PUT.
    ///
    /// FIX 4: auto-drains the send queue every 64 unsignaled posts so
    /// max_send_wr=128 is never exceeded in high-throughput PUT loops.
    pub unsafe fn post_write(
        &self,
        wr_id:        u64,
        local_offset: u32,
        len:          u32,
        remote_addr:  u64,
        remote_rkey:  u32,
    ) {
        let count = self.unsignaled_count.get();
        let (flags, drain) = if count >= 63 {
            self.unsignaled_count.set(0);
            (ibv_send_flags::IBV_SEND_SIGNALED.0, true)
        } else {
            self.unsignaled_count.set(count + 1);
            (0u32, false)
        };

        let mut sge: ibv_sge = std::mem::zeroed();
        sge.addr   = self.buf as u64 + local_offset as u64;
        sge.length = len;
        sge.lkey   = (*self.mr).lkey;

        let mut wr: ibv_send_wr = std::mem::zeroed();
        wr.wr_id      = wr_id;
        wr.opcode     = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
        wr.send_flags = flags;
        wr.sg_list    = &mut sge;
        wr.num_sge    = 1;
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey        = remote_rkey;

        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
        let rc = (*(*self.qp).context).ops
            .post_send.expect("post_send null")(self.qp, &mut wr, &mut bad_wr);
        assert_eq!(rc, 0, "ibv_post_send (WRITE) failed");

        if drain { self.poll_one(); }
    }

    /// Post one RDMA WRITE (SIGNALED).
    pub unsafe fn post_write_signaled(
        &self,
        wr_id:        u64,
        local_offset: u32,
        len:          u32,
        remote_addr:  u64,
        remote_rkey:  u32,
    ) {
        let mut sge: ibv_sge = std::mem::zeroed();
        sge.addr   = self.buf as u64 + local_offset as u64;
        sge.length = len;
        sge.lkey   = (*self.mr).lkey;

        let mut wr: ibv_send_wr = std::mem::zeroed();
        wr.wr_id      = wr_id;
        wr.opcode     = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
        wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        wr.sg_list    = &mut sge;
        wr.num_sge    = 1;
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey        = remote_rkey;

        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
        let rc = (*(*self.qp).context).ops
            .post_send.expect("post_send null")(self.qp, &mut wr, &mut bad_wr);
        assert_eq!(rc, 0, "ibv_post_send (WRITE signaled) failed");
    }

    /// Spin-poll CQ until one completion arrives. Returns the wr_id.
    pub fn poll_one(&self) -> u64 {
        unsafe {
            let mut wc: ibv_wc = std::mem::zeroed();
            let poll_fn = (*(*self.cq).context).ops
                .poll_cq.expect("poll_cq null");
            loop {
                let n = poll_fn(self.cq, 1, &mut wc);
                if n > 0 {
                    assert!(wc.is_valid(), "CQ error: {:?}", wc.error());
                    return wc.wr_id();
                }
            }
        }
    }

    /// Spin-poll until `target` completions have arrived.
    pub fn poll_n(&self, target: usize) -> Vec<u64> {
        unsafe {
            let mut ids = Vec::with_capacity(target);
            let poll_fn = (*(*self.cq).context).ops
                .poll_cq.expect("poll_cq null");
            let mut wcs: Vec<ibv_wc> = vec![std::mem::zeroed(); target.min(32).max(1)];

            while ids.len() < target {
                let want = (target - ids.len()).min(wcs.len());
                let n = poll_fn(self.cq, want as i32, wcs.as_mut_ptr());
                if n <= 0 {
                    std::hint::spin_loop();
                    continue;
                }

                for wc in &wcs[..n as usize] {
                    assert!(wc.is_valid(), "CQ error: {:?}", wc.error());
                    ids.push(wc.wr_id());
                }
            }

            ids
        }
    }
}

impl Drop for RdmaContext {
    fn drop(&mut self) {
        unsafe {
            if !self.qp.is_null()  { ibv_destroy_qp(self.qp); }
            if !self.cq.is_null()  { ibv_destroy_cq(self.cq); }
            if !self.mr.is_null()  { ibv_dereg_mr(self.mr); }
            if !self.pd.is_null()  { ibv_dealloc_pd(self.pd); }
            if !self.ctx.is_null() { ibv_close_device(self.ctx); }
            // buf_len == 0 means the buffer is caller-owned (new_with_buf).
            if !self.buf.is_null() && self.buf_len > 0 {
                std::alloc::dealloc(
                    self.buf,
                    std::alloc::Layout::from_size_align(self.buf_len, 128).unwrap(),
                );
            }
        }
    }
}
