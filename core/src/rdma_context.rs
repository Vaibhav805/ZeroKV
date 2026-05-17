// core/src/rdma_context.rs
//
// Ported to ibverbs-sys 0.3 (the raw FFI crate used by ibverbs 0.9.2).
//
// Cargo.toml must have:
//   ibverbs     = "0.9.2"
//   ibverbs-sys = "0.3"
//
// Key ibverbs-sys 0.3 conventions (all confirmed from source):
//
//  1. ibv_qp_state / ibv_qp_type / ibv_wr_opcode / ibv_wc_status / ibv_wc_opcode
//     are CONSTIFIED MODULES — their members have type `::Type` (a u32 alias).
//     Usage:  ibv_qp_state::IBV_QPS_INIT  (no .0 needed, it's already u32)
//
//  2. ibv_access_flags / ibv_qp_attr_mask / ibv_send_flags / ibv_wc_flags
//     are BITFIELD STRUCTS — constants like ibv_access_flags::IBV_ACCESS_LOCAL_WRITE are
//     ibv_access_flags(1u32).  Combine with | (operator implemented),
//     and extract the raw integer with .0 when assigning to c_uint/i32.
//
//  3. ibv_mtu is a plain Rust ENUM.
//     Fields expecting it (path_mtu) are u32 — cast with `as u32`.
//
//  4. ibv_wc.status and ibv_wc.wr_id are PRIVATE fields.
//     Use provided methods: wc.wr_id(), wc.is_valid(), wc.error().
//
//  5. ibv_query_port expects *mut _compat_ibv_port_attr, not *mut ibv_port_attr.
//     Cast:  &mut port_attr as *mut ibv_port_attr as *mut _compat_ibv_port_attr
//
//  6. ibv_post_send and ibv_poll_cq are C *inline* functions and NOT exported
//     as Rust symbols.  Call them through the context ops function pointers:
//       (*(*qp).context).ops.post_send.unwrap()(qp, wr, bad_wr)
//       (*(*cq).context).ops.poll_cq.unwrap()(cq, n, wc)

use ibverbs_sys::*;
use std::ptr;

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
    pub rkey: u32,
}

unsafe impl Send for RdmaContext {}
unsafe impl Sync for RdmaContext {}

impl RdmaContext {
    pub fn new(buf_len: usize) -> Self {
        unsafe {
            // ── device list ────────────────────────────────────────────
            let mut num_devices: i32 = 0;
            let dev_list = ibv_get_device_list(&mut num_devices);
            assert!(!dev_list.is_null() && num_devices > 0,
                    "No RDMA devices found. Is SoftRoCE / a HCA loaded?");

            let ctx = ibv_open_device(*dev_list);
            ibv_free_device_list(dev_list);
            assert!(!ctx.is_null(), "ibv_open_device failed");

            // ── protection domain ──────────────────────────────────────
            let pd = ibv_alloc_pd(ctx);
            assert!(!pd.is_null(), "ibv_alloc_pd failed");

            // ── allocate + register buffer ─────────────────────────────
            let buf = std::alloc::alloc_zeroed(
                std::alloc::Layout::from_size_align(buf_len, 64).unwrap(),
            );
            assert!(!buf.is_null(), "alloc_zeroed failed");

            // ibv_access_flags is a bitfield struct; | is implemented,
            // extract raw i32 with .0 for ibv_reg_mr's last argument.
            let access = (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE).0 as i32;

            let mr = ibv_reg_mr(pd, buf as *mut _, buf_len, access);
            assert!(!mr.is_null(), "ibv_reg_mr failed");

            // ── completion queue ───────────────────────────────────────
            let cq = ibv_create_cq(ctx, 128, ptr::null_mut(), ptr::null_mut(), 0);
            assert!(!cq.is_null(), "ibv_create_cq failed");

            // ── queue pair ─────────────────────────────────────────────
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

            // ── query port for LID ─────────────────────────────────────
            // ibv_query_port expects *mut _compat_ibv_port_attr; cast explicitly.
            let mut port_attr: ibv_port_attr = std::mem::zeroed();
            let rc = ibv_query_port(
                ctx,
                1,
                &mut port_attr as *mut ibv_port_attr as *mut _compat_ibv_port_attr,
            );
            assert_eq!(rc, 0, "ibv_query_port failed");

            let qpn  = (*qp).qp_num;
            let lid  = port_attr.lid;
            let rkey = (*mr).rkey;

            Self { ctx, pd, mr, cq, qp, buf, buf_len, qpn, lid, rkey }
        }
    }

    /// Transition QP RESET → INIT.
    pub fn move_to_init(&self) {
        unsafe {
            let mut attr: ibv_qp_attr = std::mem::zeroed();
            attr.qp_state        = ibv_qp_state::IBV_QPS_INIT;
            attr.pkey_index      = 0;
            attr.port_num        = 1;
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
    pub fn connect_rtr(&self, remote_qpn: u32, remote_lid: u16) {
        unsafe {
            let mut attr: ibv_qp_attr = std::mem::zeroed();
            attr.qp_state           = ibv_qp_state::IBV_QPS_RTR;
            // ibv_mtu is a plain Rust enum; path_mtu field is u32.
            attr.path_mtu           = IBV_MTU_1024;
            attr.dest_qp_num        = remote_qpn;
            attr.rq_psn             = 0;
            attr.max_dest_rd_atomic = 1;
            attr.min_rnr_timer      = 12;
            attr.ah_attr.dlid       = remote_lid;
            attr.ah_attr.sl         = 0;
            attr.ah_attr.src_path_bits = 0;
            attr.ah_attr.port_num   = 1;

            let mask = (ibv_qp_attr_mask::IBV_QP_STATE
                | ibv_qp_attr_mask::IBV_QP_AV
                | ibv_qp_attr_mask::IBV_QP_PATH_MTU
                | ibv_qp_attr_mask::IBV_QP_DEST_QPN
                | ibv_qp_attr_mask::IBV_QP_RQ_PSN
                | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
                | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER).0 as i32;

            let rc = ibv_modify_qp(self.qp, &mut attr, mask);
            assert_eq!(rc, 0, "RTR transition failed");
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

    /// Post one RDMA READ work-request.
    ///
    /// # Safety
    /// `local_offset + len` must lie within the registered MR.
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

        // ibv_post_send is a C inline — call via QP context ops fn pointer.
        let rc = (*(*self.qp).context)
            .ops
            .post_send
            .expect("post_send fn ptr is null")(
                self.qp,
                &mut wr,
                &mut bad_wr,
            );
        assert_eq!(rc, 0, "ibv_post_send (READ) failed");
    }

    /// Spin-poll CQ until one completion arrives. Returns the wr_id.
    pub fn poll_one(&self) -> u64 {
        unsafe {
            let mut wc: ibv_wc = std::mem::zeroed();
            // ibv_poll_cq is a C inline — call via CQ context ops fn pointer.
            let poll_fn = (*(*self.cq).context)
                .ops
                .poll_cq
                .expect("poll_cq fn ptr is null");
            loop {
                let n = poll_fn(self.cq, 1, &mut wc);
                if n > 0 {
                    // wc.status and wc.wr_id are private; use accessor methods.
                    assert!(wc.is_valid(), "CQ error: {:?}", wc.error());
                    return wc.wr_id();
                }
            }
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
            if !self.buf.is_null() {
                std::alloc::dealloc(
                    self.buf,
                    std::alloc::Layout::from_size_align(self.buf_len, 64).unwrap(),
                );
            }
        }
    }
}