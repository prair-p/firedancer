#include "tiles.h"

#include "generated/plugin_seccomp.h"

#include "../../../../disco/plugin/fd_plugin.h"
#include "../../../../flamenco/types/fd_types.h"

typedef struct {
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
} fd_plugin_in_ctx_t;

typedef struct {
  fd_plugin_in_ctx_t in[ 64UL ];

  uchar buf[ 8UL+40200UL*(58UL+12UL*34UL) ] __attribute__((aligned(8)));

  fd_wksp_t * out_mem;
  ulong       out_chunk0;
  ulong       out_wmark;
  ulong       out_chunk;
} fd_plugin_ctx_t;

FD_FN_CONST static inline ulong
scratch_align( void ) {
  return 128UL;
}

FD_FN_PURE static inline ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof( fd_plugin_ctx_t ), sizeof( fd_plugin_ctx_t ) );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

FD_FN_CONST static inline void *
mux_ctx( void * scratch ) {
  return (void*)fd_ulong_align_up( (ulong)scratch, alignof( fd_plugin_ctx_t ) );
}

static inline void
during_frag( void * _ctx,
             ulong  in_idx,
             ulong  seq,
             ulong  sig,
             ulong  chunk,
             ulong  sz,
             int *  opt_filter ) {
  (void)seq;
  (void)sig;
  (void)opt_filter;

  fd_plugin_ctx_t * ctx = (fd_plugin_ctx_t *)_ctx;

  uchar * src = (uchar *)fd_chunk_to_laddr( ctx->in[ in_idx ].mem, chunk );
  ulong * dst = (ulong *)fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk );

  ulong _sz = sz;
  if( FD_UNLIKELY( in_idx==1UL ) ) _sz = 8UL + 40200UL*(58UL+12UL*34UL);
  else if( FD_UNLIKELY( in_idx==2UL ) ) _sz = 40UL + 40200UL*40UL; /* ... todo... sigh, sz is not correct since it's too big */
  fd_memcpy( dst, src, _sz );
}

static inline void
after_frag( void *             _ctx,
            ulong              in_idx,
            ulong              seq,
            ulong *            opt_sig,
            ulong *            opt_chunk,
            ulong *            opt_sz,
            ulong *            opt_tsorig,
            int *              opt_filter,
            fd_mux_context_t * mux ) {
  (void)in_idx;
  (void)seq;
  (void)opt_chunk;
  (void)opt_sz;
  (void)opt_tsorig;
  (void)opt_filter;
  (void)mux;

  fd_plugin_ctx_t * ctx = (fd_plugin_ctx_t *)_ctx;

  ulong sig;
  switch( in_idx ) {
    /* replay_plugin */
    case 0UL: {
      FD_TEST( *opt_sig==FD_PLUGIN_MSG_SLOT_ROOTED || *opt_sig==FD_PLUGIN_MSG_SLOT_OPTIMISTICALLY_CONFIRMED || *opt_sig==FD_PLUGIN_MSG_SLOT_COMPLETED || *opt_sig==FD_PLUGIN_MSG_SLOT_RESET );
      sig = *opt_sig;
      break;
    }
    /* gossip_plugin */
    case 1UL: {
      FD_TEST( *opt_sig==FD_PLUGIN_MSG_GOSSIP_UPDATE || *opt_sig==FD_PLUGIN_MSG_VOTE_ACCOUNT_UPDATE || *opt_sig==FD_PLUGIN_MSG_VALIDATOR_INFO || *opt_sig==FD_PLUGIN_MSG_BALANCE );
      sig = *opt_sig;
      break;
    }
    /* stake_out */
    case 2UL: sig = FD_PLUGIN_MSG_LEADER_SCHEDULE; FD_LOG_NOTICE(( "sending leader schedule" )); break;
    /* poh_plugin */
    case 3UL: {
      FD_TEST( *opt_sig==FD_PLUGIN_MSG_SLOT_START || *opt_sig==FD_PLUGIN_MSG_SLOT_END );
      sig = *opt_sig;
      break;
    }
    /* startp_plugi */
    case 4UL: {
      FD_TEST( *opt_sig==FD_PLUGIN_MSG_START_PROGRESS );
      sig = *opt_sig;
      break;
    }
    /* votel_plugin*/
    case 5UL: {
      FD_TEST( *opt_sig==FD_PLUGIN_MSG_SLOT_OPTIMISTICALLY_CONFIRMED );
      sig = *opt_sig;
      break;
    }
    default: FD_LOG_ERR(( "bad in_idx" ));
  }

  ulong _sz = *opt_sz;
  if( FD_UNLIKELY( in_idx==1UL ) ) _sz = 8UL + 40200UL*(58UL+12UL*34UL);
  else if( FD_UNLIKELY( in_idx==2UL ) ) _sz = 40UL + 40200UL*40UL; /* ... todo... sigh, sz is not correct since it's too big */

  fd_mux_publish( mux, sig, ctx->out_chunk, *opt_sz, 0UL, 0UL, 0UL );
  ctx->out_chunk = fd_dcache_compact_next( ctx->out_chunk, _sz, ctx->out_chunk0, ctx->out_wmark );
}

static void
unprivileged_init( fd_topo_t *      topo,
                   fd_topo_tile_t * tile,
                   void *           scratch ) {
  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_plugin_ctx_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_plugin_ctx_t ), sizeof( fd_plugin_ctx_t ) );

  FD_TEST( tile->in_cnt<=sizeof( ctx->in )/sizeof( ctx->in[ 0 ] ) );
  for( ulong i=0; i<tile->in_cnt; i++ ) {
    fd_topo_link_t * link = &topo->links[ tile->in_link_id[ i ] ];
    fd_topo_wksp_t * link_wksp = &topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ];

    ctx->in[ i ].mem    = link_wksp->wksp;
    ctx->in[ i ].chunk0 = fd_dcache_compact_chunk0( ctx->in[ i ].mem, link->dcache );
    ctx->in[ i ].wmark  = fd_dcache_compact_wmark ( ctx->in[ i ].mem, link->dcache, link->mtu );
  }

  ctx->out_mem    = topo->workspaces[ topo->objs[ topo->links[ tile->out_link_id_primary ].dcache_obj_id ].wksp_id ].wksp;
  ctx->out_chunk0 = fd_dcache_compact_chunk0( ctx->out_mem, topo->links[ tile->out_link_id_primary ].dcache );
  ctx->out_wmark  = fd_dcache_compact_wmark ( ctx->out_mem, topo->links[ tile->out_link_id_primary ].dcache, topo->links[ tile->out_link_id_primary ].mtu );
  ctx->out_chunk  = ctx->out_chunk0;

  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, 1UL );
  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu", scratch_top - (ulong)scratch - scratch_footprint( tile ), scratch_top, (ulong)scratch + scratch_footprint( tile ) ));
}

static ulong
populate_allowed_seccomp( void *               scratch,
                          ulong                out_cnt,
                          struct sock_filter * out ) {
  (void)scratch;

  populate_sock_filter_policy_plugin( out_cnt, out, (uint)fd_log_private_logfile_fd() );
  return sock_filter_policy_plugin_instr_cnt;
}

static ulong
populate_allowed_fds( void * scratch,
                      ulong  out_fds_cnt,
                      int *  out_fds ) {
  (void)scratch;

  if( FD_UNLIKELY( out_fds_cnt<2UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  return out_cnt;
}

fd_topo_run_tile_t fd_tile_plugin = {
  .name                     = "plugin",
  .mux_flags                = FD_MUX_FLAG_COPY | FD_MUX_FLAG_MANUAL_PUBLISH,
  .burst                    = 1UL,
  .rlimit_file_cnt          = 0UL,
  .mux_ctx                  = mux_ctx,
  .mux_during_frag          = during_frag,
  .mux_after_frag           = after_frag,
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .unprivileged_init        = unprivileged_init,
};
