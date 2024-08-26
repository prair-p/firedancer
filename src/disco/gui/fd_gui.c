#include "fd_gui.h"
#include "fd_gui_printf.h"

#include "../fd_disco.h"
#include "../plugin/fd_plugin.h"

#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/json/cJSON.h"

#include "../../app/fdctl/config.h"

FD_FN_CONST ulong
fd_gui_align( void ) {
  return 128UL;
}

FD_FN_CONST ulong
fd_gui_footprint( void ) {
  return sizeof(fd_gui_t);
}

void *
fd_gui_new( void *        shmem,
            fd_hcache_t * hcache,
            char const *  version,
            char const *  cluster,
            uchar const * identity_key,
            fd_topo_t *   topo ) {

  if( FD_UNLIKELY( !shmem ) ) {
    FD_LOG_WARNING(( "NULL shmem" ));
    return NULL;
  }

  if( FD_UNLIKELY( !fd_ulong_is_aligned( (ulong)shmem, fd_gui_align() ) ) ) {
    FD_LOG_WARNING(( "misaligned shmem" ));
    return NULL;
  }

  if( FD_UNLIKELY( topo->tile_cnt>64UL ) ) {
    FD_LOG_WARNING(( "too many tiles" ));
    return NULL;
  }

  fd_gui_t * gui = (fd_gui_t *)shmem;

  gui->hcache = hcache;
  gui->topo   = topo;

  gui->next_sample_100millis = fd_log_wallclock();
  gui->next_sample_10millis  = fd_log_wallclock();

  memcpy( gui->summary.identity_key->uc, identity_key, 32UL );
  fd_base58_encode_32( identity_key, NULL, gui->summary.identity_key_base58 );
  gui->summary.identity_key_base58[ FD_BASE58_ENCODED_32_SZ-1UL ] = '\0';

  gui->summary.version                       = version;
  gui->summary.cluster                       = cluster;
  gui->summary.startup_time_nanos            = fd_log_wallclock();
  gui->summary.balance                       = 0UL;
  gui->summary.estimated_slot_duration_nanos = 0UL;

  gui->summary.net_tile_cnt    = fd_topo_tile_name_cnt( gui->topo, "net"    );
  gui->summary.quic_tile_cnt   = fd_topo_tile_name_cnt( gui->topo, "quic"   );
  gui->summary.verify_tile_cnt = fd_topo_tile_name_cnt( gui->topo, "verify" );
  gui->summary.bank_tile_cnt   = fd_topo_tile_name_cnt( gui->topo, "bank"   );
  gui->summary.shred_tile_cnt  = fd_topo_tile_name_cnt( gui->topo, "shred"  );

  gui->summary.slot_rooted                   = 0UL;
  gui->summary.slot_optimistically_confirmed = 0UL;
  gui->summary.slot_completed                = 0UL;
  gui->summary.slot_estimated                = 0UL;

  gui->summary.estimated_tps        = 0UL;
  gui->summary.estimated_vote_tps   = 0UL;
  gui->summary.estimated_failed_tps = 0UL;

  memset( gui->summary.txn_waterfall_reference, 0, sizeof(gui->summary.txn_waterfall_reference) );
  memset( gui->summary.txn_waterfall_current,   0, sizeof(gui->summary.txn_waterfall_current) );

  memset( gui->summary.tile_timers_snap[ 0 ], 0, sizeof(gui->summary.tile_timers_snap[ 0 ]) );
  memset( gui->summary.tile_timers_snap[ 1 ], 0, sizeof(gui->summary.tile_timers_snap[ 1 ]) );
  gui->summary.tile_timers_snap_idx = 2UL;

  gui->epoch.has_epoch[ 0 ] = 0;
  gui->epoch.has_epoch[ 1 ] = 0;

  gui->gossip.peer_cnt               = 0UL;
  gui->vote_account.vote_account_cnt = 0UL;
  gui->validator_info.info_cnt       = 0UL;

  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);
  for( ulong i=0UL; i<slots_sz; i++ ) gui->slots[ i ]->slot = ULONG_MAX;

  return gui;
}

fd_gui_t *
fd_gui_join( void * shmem ) {
  return (fd_gui_t *)shmem;
}

void
fd_gui_ws_open( fd_gui_t * gui,
                ulong      ws_conn_id ) {
  void (* printers[] )( fd_gui_t * gui ) = {
    fd_gui_printf_version,
    fd_gui_printf_cluster,
    fd_gui_printf_identity_key,
    fd_gui_printf_uptime_nanos,
    fd_gui_printf_net_tile_count,
    fd_gui_printf_quic_tile_count,
    fd_gui_printf_verify_tile_count,
    fd_gui_printf_bank_tile_count,
    fd_gui_printf_shred_tile_count,
    fd_gui_printf_balance,
    fd_gui_printf_estimated_slot_duration_nanos,
    fd_gui_printf_root_slot,
    fd_gui_printf_optimistically_confirmed_slot,
    fd_gui_printf_completed_slot,
    fd_gui_printf_estimated_slot,
    fd_gui_printf_live_tile_timers,
    fd_gui_printf_peers_all,
  };

  ulong printers_len = sizeof(printers) / sizeof(printers[0]);
  for( ulong i=0UL; i<printers_len; i++ ) {
    printers[ i ]( gui );
    FD_TEST( !fd_hcache_snap_ws_send( gui->hcache, ws_conn_id ) );
  }

  for( ulong i=0UL; i<2UL; i++ ) {
    if( FD_LIKELY( gui->epoch.has_epoch[ i ] ) ) {
      fd_gui_printf_epoch( gui, i );
      FD_TEST( !fd_hcache_snap_ws_send( gui->hcache, ws_conn_id ) );
    }
  }
}

static void
fd_gui_tile_timers_snap( fd_gui_t *             gui,
                         fd_gui_tile_timers_t * cur ) {
  for( ulong i=0UL; i<gui->topo->tile_cnt; i++ ) {
    fd_topo_tile_t * tile = &gui->topo->tiles[ i ];
    ulong const * tile_metrics = fd_metrics_tile( tile->metrics );

    cur[ i ].housekeeping_ticks       = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_HOUSEKEEPING_DURATION_SECONDS ) ];
    cur[ i ].backpressure_ticks       = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_BACKPRESSURE_DURATION_SECONDS ) ];
    cur[ i ].caught_up_ticks          = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_CAUGHT_UP_DURATION_SECONDS ) ];
    cur[ i ].overrun_polling_ticks    = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_OVERRUN_POLLING_DURATION_SECONDS ) ];
    cur[ i ].overrun_reading_ticks    = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_OVERRUN_READING_DURATION_SECONDS ) ];
    cur[ i ].filter_before_frag_ticks = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_FILTER_BEFORE_FRAGMENT_DURATION_SECONDS ) ];
    cur[ i ].filter_after_frag_ticks  = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_FILTER_AFTER_FRAGMENT_DURATION_SECONDS ) ];
    cur[ i ].finish_ticks             = tile_metrics[ FD_HISTF_BUCKET_CNT + MIDX( HISTOGRAM, STEM, LOOP_FINISH_DURATION_SECONDS ) ];
  }
}

/* Snapshot all of the data from metrics to construct a view of the
   transaction waterfall.

   Tiles are sampled in reverse pipeline order: this helps prevent data
   discrepancies where a later tile has "seen" more transactions than an
   earlier tile, which shouldn't typically happen. */

static void
fd_gui_txn_waterfall_snap( fd_gui_t *               gui,
                           fd_gui_txn_waterfall_t * cur ) {
  fd_topo_t * topo = gui->topo;

  cur->out.block_success = 0UL;
  cur->out.block_fail    = 0UL;

  cur->out.bank_invalid = 0UL;
  for( ulong i=0UL; i<gui->summary.bank_tile_cnt; i++ ) {
    fd_topo_tile_t const * bank = &topo->tiles[ fd_topo_find_tile( topo, "bank", i ) ];

    ulong const * bank_metrics = fd_metrics_tile( bank->metrics );

    cur->out.block_success += bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_SUCCESS ) ];

    cur->out.block_fail +=
        bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_ACCOUNT_IN_USE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_ACCOUNT_LOADED_TWICE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_ACCOUNT_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_PROGRAM_ACCOUNT_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INSUFFICIENT_FUNDS_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_ACCOUNT_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_ALREADY_PROCESSED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_BLOCKHASH_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INSTRUCTION_ERROR ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_CALL_CHAIN_TOO_DEEP ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_MISSING_SIGNATURE_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_ACCOUNT_INDEX ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_SIGNATURE_FAILURE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_PROGRAM_FOR_EXECUTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_SANITIZE_FAILURE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_CLUSTER_MAINTENANCE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_ACCOUNT_BORROW_OUTSTANDING ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_UNSUPPORTED_VERSION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_WRITABLE_ACCOUNT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_TOO_MANY_ACCOUNT_LOCKS ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_ADDRESS_LOOKUP_TABLE_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_ADDRESS_LOOKUP_TABLE_OWNER ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_ADDRESS_LOOKUP_TABLE_DATA ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_ADDRESS_LOOKUP_TABLE_INDEX ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_RENT_PAYING_ACCOUNT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_WOULD_EXCEED_MAX_VOTE_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_DUPLICATE_INSTRUCTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INSUFFICIENT_FUNDS_FOR_RENT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_RESANITIZATION_NEEDED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_UNBALANCED_TRANSACTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTED_PROGRAM_CACHE_HIT_MAX_LIMIT ) ];

    cur->out.bank_invalid +=
        bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ADDRESS_TABLES_SLOT_HASHES_SYSVAR_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ADDRESS_TABLES_ACCOUNT_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ADDRESS_TABLES_INVALID_ACCOUNT_OWNER ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ADDRESS_TABLES_INVALID_ACCOUNT_DATA ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ADDRESS_TABLES_INVALID_INDEX ) ];

    cur->out.bank_invalid +=
        bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ACCOUNT_IN_USE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ACCOUNT_LOADED_TWICE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ACCOUNT_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_PROGRAM_ACCOUNT_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INSUFFICIENT_FUNDS_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_ACCOUNT_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ALREADY_PROCESSED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_BLOCKHASH_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INSTRUCTION_ERROR ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_CALL_CHAIN_TOO_DEEP ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_MISSING_SIGNATURE_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_ACCOUNT_INDEX ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_SIGNATURE_FAILURE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_PROGRAM_FOR_EXECUTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_SANITIZE_FAILURE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_CLUSTER_MAINTENANCE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ACCOUNT_BORROW_OUTSTANDING ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_UNSUPPORTED_VERSION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_WRITABLE_ACCOUNT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_TOO_MANY_ACCOUNT_LOCKS ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_ADDRESS_LOOKUP_TABLE_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_ADDRESS_LOOKUP_TABLE_OWNER ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_ADDRESS_LOOKUP_TABLE_DATA ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_ADDRESS_LOOKUP_TABLE_INDEX ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_RENT_PAYING_ACCOUNT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_WOULD_EXCEED_MAX_VOTE_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_DUPLICATE_INSTRUCTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INSUFFICIENT_FUNDS_FOR_RENT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_RESANITIZATION_NEEDED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_UNBALANCED_TRANSACTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_LOAD_PROGRAM_CACHE_HIT_MAX_LIMIT ) ];

    cur->out.bank_invalid +=
        bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_ACCOUNT_IN_USE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_ACCOUNT_LOADED_TWICE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_ACCOUNT_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_PROGRAM_ACCOUNT_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INSUFFICIENT_FUNDS_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_ACCOUNT_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_ALREADY_PROCESSED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_BLOCKHASH_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INSTRUCTION_ERROR ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_CALL_CHAIN_TOO_DEEP ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_MISSING_SIGNATURE_FOR_FEE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_ACCOUNT_INDEX ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_SIGNATURE_FAILURE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_PROGRAM_FOR_EXECUTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_SANITIZE_FAILURE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_CLUSTER_MAINTENANCE ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_ACCOUNT_BORROW_OUTSTANDING ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_UNSUPPORTED_VERSION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_WRITABLE_ACCOUNT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_TOO_MANY_ACCOUNT_LOCKS ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_ADDRESS_LOOKUP_TABLE_NOT_FOUND ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_ADDRESS_LOOKUP_TABLE_OWNER ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_ADDRESS_LOOKUP_TABLE_DATA ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_ADDRESS_LOOKUP_TABLE_INDEX ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_RENT_PAYING_ACCOUNT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_WOULD_EXCEED_MAX_VOTE_COST_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_DUPLICATE_INSTRUCTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INSUFFICIENT_FUNDS_FOR_RENT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_RESANITIZATION_NEEDED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_UNBALANCED_TRANSACTION ) ]
      + bank_metrics[ MIDX( COUNTER, BANK_TILE, TRANSACTION_EXECUTING_PROGRAM_CACHE_HIT_MAX_LIMIT ) ];
  }


  fd_topo_tile_t const * pack = &topo->tiles[ fd_topo_find_tile( topo, "pack", 0UL ) ];
  ulong const * pack_metrics = fd_metrics_tile( pack->metrics );

  cur->out.pack_invalid =
      pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_WRITE_SYSVAR ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_ESTIMATION_FAIL ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_DUPLICATE_ACCOUNT ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_TOO_MANY_ACCOUNTS ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_TOO_LARGE ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_EXPIRED ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_ADDR_LUT ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_UNAFFORDABLE ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_DUPLICATE ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_EXPIRED ) ];

  cur->out.pack_leader_slow =
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_PRIORITY ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_NONVOTE_REPLACE ) ] +
    + pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_INSERTED_VOTE_REPLACE ) ];

  cur->out.pack_wait_full =
      pack_metrics[ MIDX( COUNTER, PACK, TRANSACTION_DROPPED_FROM_EXTRA ) ];

  cur->out.pack_retained = pack_metrics[ MIDX( GAUGE, PACK, AVAILABLE_TRANSACTIONS ) ];


  fd_topo_tile_t const * dedup = &topo->tiles[ fd_topo_find_tile( topo, "dedup", 0UL ) ];
  ulong const * dedup_metrics = fd_metrics_tile( dedup->metrics );

  cur->out.dedup_duplicate = 0UL;
  for( ulong i=0UL; i<gui->summary.verify_tile_cnt; i++ ) {
    cur->out.dedup_duplicate += fd_metrics_link_in( dedup->metrics, i )[ FD_METRICS_COUNTER_LINK_FILTERED_COUNT_OFF ];
  }

  
  cur->out.verify_overrun   = 0UL;
  cur->out.verify_duplicate = 0UL;
  cur->out.verify_parse     = 0UL;
  cur->out.verify_failed    = 0UL;

  for( ulong i=0UL; i<gui->summary.verify_tile_cnt; i++ ) {
    fd_topo_tile_t const * verify = &topo->tiles[ fd_topo_find_tile( topo, "verify", i ) ];
    ulong const * verify_metrics = fd_metrics_tile( verify->metrics );

    for( ulong j=0UL; j<gui->summary.quic_tile_cnt; j++ ) {
      /* TODO: Not precise... even if 1 frag gets skipped, it could have been for this verify tile. */
      cur->out.verify_overrun += fd_metrics_link_in( verify->metrics, j )[ FD_METRICS_COUNTER_LINK_OVERRUN_POLLING_FRAG_COUNT_OFF ] / gui->summary.verify_tile_cnt;
      cur->out.verify_overrun += fd_metrics_link_in( verify->metrics, j )[ FD_METRICS_COUNTER_LINK_OVERRUN_READING_FRAG_COUNT_OFF ];

      cur->out.verify_failed    += verify_metrics[ MIDX( COUNTER, VERIFY, TRANSACTION_VERIFY_FAILURE ) ];
      cur->out.verify_parse     += verify_metrics[ MIDX( COUNTER, VERIFY, TRANSACTION_PARSE_FAILURE ) ];
      cur->out.verify_duplicate += verify_metrics[ MIDX( COUNTER, VERIFY, TRANSACTION_DEDUP_FAILURE ) ];
    }
  }


  cur->out.quic_overrun      = 0UL;
  cur->out.quic_quic_invalid = 0UL;
  cur->out.quic_udp_invalid  = 0UL;
  for( ulong i=0UL; i<gui->summary.quic_tile_cnt; i++ ) {
    fd_topo_tile_t const * quic = &topo->tiles[ fd_topo_find_tile( topo, "quic", i ) ];
    ulong * quic_metrics = fd_metrics_tile( quic->metrics );

    cur->out.quic_udp_invalid  += quic_metrics[ MIDX( COUNTER, QUIC_TILE, NON_QUIC_PACKET_TOO_SMALL ) ];
    cur->out.quic_udp_invalid  += quic_metrics[ MIDX( COUNTER, QUIC_TILE, NON_QUIC_PACKET_TOO_LARGE ) ];
    cur->out.quic_udp_invalid  += quic_metrics[ MIDX( COUNTER, QUIC_TILE, NON_QUIC_REASSEMBLY_PUBLISH_ERROR_OVERSIZE ) ];
    cur->out.quic_udp_invalid  += quic_metrics[ MIDX( COUNTER, QUIC_TILE, NON_QUIC_REASSEMBLY_PUBLISH_ERROR_SKIP ) ];
    cur->out.quic_udp_invalid  += quic_metrics[ MIDX( COUNTER, QUIC_TILE, NON_QUIC_REASSEMBLY_PUBLISH_ERROR_STATE ) ];

    cur->out.quic_quic_invalid += quic_metrics[ MIDX( COUNTER, QUIC_TILE, QUIC_PACKET_TOO_SMALL ) ];
    cur->out.quic_quic_invalid += quic_metrics[ MIDX( COUNTER, QUIC_TILE, REASSEMBLY_PUBLISH_ERROR_OVERSIZE ) ];
    cur->out.quic_quic_invalid += quic_metrics[ MIDX( COUNTER, QUIC_TILE, REASSEMBLY_PUBLISH_ERROR_SKIP ) ];
    cur->out.quic_quic_invalid += quic_metrics[ MIDX( COUNTER, QUIC_TILE, REASSEMBLY_PUBLISH_ERROR_STATE ) ];

    cur->out.quic_overrun      += quic_metrics[ MIDX( COUNTER, QUIC_TILE, REASSEMBLY_NOTIFY_CLOBBERED ) ];

    for( ulong j=0UL; j<gui->summary.net_tile_cnt; j++ ) {
      /* TODO: Not precise... net frags that were skipped might not have been destined for QUIC tile */
      /* TODO: Not precise... even if 1 frag gets skipped, it could have been for this QUIC tile */
      cur->out.quic_overrun += fd_metrics_link_in( quic->metrics, j )[ FD_METRICS_COUNTER_LINK_OVERRUN_POLLING_FRAG_COUNT_OFF ] / gui->summary.quic_tile_cnt;
      cur->out.quic_overrun += fd_metrics_link_in( quic->metrics, j )[ FD_METRICS_COUNTER_LINK_OVERRUN_READING_FRAG_COUNT_OFF ];
    }
  }

  cur->in.gossip   = dedup_metrics[ MIDX( COUNTER, DEDUP, GOSSIPED_VOTES_RECEIVED ) ];
  cur->in.quic     = cur->out.quic_quic_invalid+cur->out.quic_overrun;
  cur->in.udp      = cur->out.quic_udp_invalid;
  for( ulong i=0UL; i<gui->summary.quic_tile_cnt; i++ ) {
    fd_topo_tile_t const * quic = &topo->tiles[ fd_topo_find_tile( topo, "quic", i ) ];
    ulong * quic_metrics = fd_metrics_tile( quic->metrics );

    cur->in.quic += quic_metrics[ MIDX( COUNTER, QUIC_TILE, REASSEMBLY_PUBLISH_SUCCESS ) ];
    cur->in.udp  += quic_metrics[ MIDX( COUNTER, QUIC_TILE, NON_QUIC_REASSEMBLY_PUBLISH_SUCCESS ) ];
  }

  /* TODO: We can get network packet drops between the device and the
           kernel ring buffer by querying some network device stats... */
}

void
fd_gui_poll( fd_gui_t * gui ) {
  long now = fd_log_wallclock();

  if( FD_LIKELY( now>gui->next_sample_100millis ) ) {
    fd_gui_txn_waterfall_snap( gui, gui->summary.txn_waterfall_current );

    fd_gui_printf_live_txn_waterfall( gui, gui->summary.txn_waterfall_reference, gui->summary.txn_waterfall_current, 0UL /* TODO: REAL NEXT LEADER SLOT */ );
    fd_hcache_snap_ws_broadcast( gui->hcache );

    gui->next_sample_100millis += 100L*1000L*1000L;
  }

  if( FD_LIKELY( now>gui->next_sample_10millis ) ) {
    fd_gui_tile_timers_snap( gui, gui->summary.tile_timers_snap[ gui->summary.tile_timers_snap_idx ]);
    gui->summary.tile_timers_snap_idx = (gui->summary.tile_timers_snap_idx+1UL) % (sizeof(gui->summary.tile_timers_snap)/sizeof(gui->summary.tile_timers_snap[ 0 ]));

    fd_gui_printf_live_tile_timers( gui );
    fd_hcache_snap_ws_broadcast( gui->hcache );

    gui->next_sample_10millis += 10L*1000L*1000L;
  }
}

static void
fd_gui_handle_gossip_update( fd_gui_t *    gui,
                             uchar const * msg ) {
  ulong const * header = (ulong const *)fd_type_pun_const( msg );
  ulong peer_cnt = header[ 0 ];

  ulong added_cnt = 0UL;
  ulong added[ 40200 ] = {0};

  ulong update_cnt = 0UL;
  ulong updated[ 40200 ] = {0};

  ulong removed_cnt = 0UL;
  fd_pubkey_t removed[ 40200 ] = {0};

  uchar const * data = (uchar const *)(header+1UL);
  for( ulong i=0UL; i<gui->gossip.peer_cnt; i++ ) {
    int found = 0;
    for( ulong j=0UL; j<peer_cnt; j++ ) {
      if( FD_UNLIKELY( !memcmp( gui->gossip.peers[ i ].pubkey, data+j*(58UL+12UL*6UL), 32UL ) ) ) {
        found = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found ) ) {
      fd_memcpy( removed[ removed_cnt++ ].uc, gui->gossip.peers[ i ].pubkey->uc, 32UL );
      if( FD_LIKELY( i+1UL!=gui->gossip.peer_cnt ) ) {
        fd_memcpy( &gui->gossip.peers[ i ], &gui->gossip.peers[ gui->gossip.peer_cnt-1UL ], sizeof(struct fd_gui_gossip_peer) );
        gui->gossip.peer_cnt--;
        i--;
      }
    }
  }

  ulong before_peer_cnt = gui->gossip.peer_cnt;
  for( ulong i=0UL; i<peer_cnt; i++ ) {
    int found = 0;
    ulong found_idx;
    for( ulong j=0UL; j<gui->gossip.peer_cnt; j++ ) {
      if( FD_UNLIKELY( !memcmp( gui->gossip.peers[ j ].pubkey, data+i*(58UL+12UL*6UL), 32UL ) ) ) {
        found_idx = j;
        found = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found ) ) {
      fd_memcpy( gui->gossip.peers[ gui->gossip.peer_cnt ].pubkey->uc, data+i*(58UL+12UL*6UL), 32UL );
      gui->gossip.peers[ gui->gossip.peer_cnt ].wallclock = *(ulong const *)(data+i*(58UL+12UL*6UL)+32UL);
      gui->gossip.peers[ gui->gossip.peer_cnt ].shred_version = *(ushort const *)(data+i*(58UL+12UL*6UL)+40UL);
      gui->gossip.peers[ gui->gossip.peer_cnt ].has_version = *(data+i*(58UL+12UL*6UL)+42UL);
      if( FD_LIKELY( gui->gossip.peers[ gui->gossip.peer_cnt ].has_version ) ) {
        gui->gossip.peers[ gui->gossip.peer_cnt ].version.major = *(ushort const *)(data+i*(58UL+12UL*6UL)+43UL);
        gui->gossip.peers[ gui->gossip.peer_cnt ].version.minor = *(ushort const *)(data+i*(58UL+12UL*6UL)+45UL);
        gui->gossip.peers[ gui->gossip.peer_cnt ].version.patch = *(ushort const *)(data+i*(58UL+12UL*6UL)+47UL);
        gui->gossip.peers[ gui->gossip.peer_cnt ].version.has_commit = *(data+i*(58UL+12UL*6UL)+49UL);
        if( FD_LIKELY( gui->gossip.peers[ gui->gossip.peer_cnt ].version.has_commit ) ) {
          gui->gossip.peers[ gui->gossip.peer_cnt ].version.commit = *(uint const *)(data+i*(58UL+12UL*6UL)+50UL);
        }
        gui->gossip.peers[ gui->gossip.peer_cnt ].version.feature_set = *(uint const *)(data+i*(58UL+12UL*6UL)+54UL);
      }

      for( ulong j=0UL; j<12UL; j++ ) {
        gui->gossip.peers[ gui->gossip.peer_cnt ].sockets[ j ].ipv4 = *(uint const *)(data+i*(58UL+12UL*6UL)+58UL+j*6UL);
        gui->gossip.peers[ gui->gossip.peer_cnt ].sockets[ j ].port = *(ushort const *)(data+i*(58UL+12UL*6UL)+58UL+j*6UL+4UL);
      }

      gui->gossip.peer_cnt++;
    } else {
      int peer_updated = gui->gossip.peers[ gui->gossip.peer_cnt ].shred_version!=*(ushort const *)(data+i*(58UL+12UL*6UL)+40UL) ||
                          gui->gossip.peers[ gui->gossip.peer_cnt ].wallclock!=*(ulong const *)(data+i*(58UL+12UL*6UL)+32UL) ||
                          gui->gossip.peers[ gui->gossip.peer_cnt ].has_version!=*(data+i*(58UL+12UL*6UL)+42UL);
      if( FD_LIKELY( !peer_updated && gui->gossip.peers[ gui->gossip.peer_cnt ].has_version ) ) {
        peer_updated = gui->gossip.peers[ gui->gossip.peer_cnt ].version.major!=*(ushort const *)(data+i*(58UL+12UL*6UL)+43UL) ||
                        gui->gossip.peers[ gui->gossip.peer_cnt ].version.minor!=*(ushort const *)(data+i*(58UL+12UL*6UL)+45UL) ||
                        gui->gossip.peers[ gui->gossip.peer_cnt ].version.patch!=*(ushort const *)(data+i*(58UL+12UL*6UL)+47UL) ||
                        gui->gossip.peers[ gui->gossip.peer_cnt ].version.has_commit!=*(data+i*(58UL+12UL*6UL)+49UL) ||
                        (gui->gossip.peers[ gui->gossip.peer_cnt ].version.has_commit && gui->gossip.peers[ gui->gossip.peer_cnt ].version.commit!=*(uint const *)(data+i*(58UL+12UL*6UL)+50UL)) ||
                        gui->gossip.peers[ gui->gossip.peer_cnt ].version.feature_set!=*(uint const *)(data+i*(58UL+12UL*6UL)+54UL);

        if( FD_LIKELY( !peer_updated ) ) {
          for( ulong j=0UL; j<12UL; j++ ) {
            peer_updated = gui->gossip.peers[ gui->gossip.peer_cnt ].sockets[ j ].ipv4!=*(uint const *)(data+i*(58UL+12UL*6UL)+58UL+j*6UL) ||
                            gui->gossip.peers[ gui->gossip.peer_cnt ].sockets[ j ].port!=*(ushort const *)(data+i*(58UL+12UL*6UL)+58UL+j*6UL+4UL);
            if( FD_LIKELY( peer_updated ) ) break;
          }
        }
      }

      if( FD_UNLIKELY( peer_updated ) ) {
        updated[ update_cnt++ ] = found_idx;
        gui->gossip.peers[ found_idx ].shred_version = *(ushort const *)(data+i*(58UL+12UL*6UL)+40UL);
        gui->gossip.peers[ found_idx ].wallclock = *(ulong const *)(data+i*(58UL+12UL*6UL)+32UL);
        gui->gossip.peers[ found_idx ].has_version = *(data+i*(58UL+12UL*6UL)+42UL);
        if( FD_LIKELY( gui->gossip.peers[ found_idx ].has_version ) ) {
          gui->gossip.peers[ found_idx ].version.major = *(ushort const *)(data+i*(58UL+12UL*6UL)+43UL);
          gui->gossip.peers[ found_idx ].version.minor = *(ushort const *)(data+i*(58UL+12UL*6UL)+45UL);
          gui->gossip.peers[ found_idx ].version.patch = *(ushort const *)(data+i*(58UL+12UL*6UL)+47UL);
          gui->gossip.peers[ found_idx ].version.has_commit = *(data+i*(58UL+12UL*6UL)+49UL);
          if( FD_LIKELY( gui->gossip.peers[ found_idx ].version.has_commit ) ) {
            gui->gossip.peers[ found_idx ].version.commit = *(uint const *)(data+i*(58UL+12UL*6UL)+50UL);
          }
          gui->gossip.peers[ gui->gossip.peer_cnt ].version.feature_set = *(uint const *)(data+i*(58UL+12UL*6UL)+54UL);
        }

        for( ulong j=0UL; j<12UL; j++ ) {
          gui->gossip.peers[ found_idx ].sockets[ j ].ipv4 = *(uint const *)(data+i*(58UL+12UL*6UL)+58UL+j*6UL);
          gui->gossip.peers[ found_idx ].sockets[ j ].port = *(ushort const *)(data+i*(58UL+12UL*6UL)+58UL+j*6UL+4UL);
        }
      }
    }
  }

  added_cnt = gui->gossip.peer_cnt - before_peer_cnt;
  for( ulong i=before_peer_cnt; i<gui->gossip.peer_cnt; i++ ) added[ i-before_peer_cnt ] = i;

  fd_gui_printf_peers_gossip_update( gui, updated, update_cnt, removed, removed_cnt, added, added_cnt );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

static void
fd_gui_handle_vote_account_update( fd_gui_t *    gui,
                                   uchar const * msg ) {
  ulong const * header = (ulong const *)fd_type_pun_const( msg );
  ulong peer_cnt = header[ 0 ];

  ulong added_cnt = 0UL;
  ulong added[ 40200 ] = {0};

  ulong update_cnt = 0UL;
  ulong updated[ 40200 ] = {0};

  ulong removed_cnt = 0UL;
  fd_pubkey_t removed[ 40200 ] = {0};

  uchar const * data = (uchar const *)(header+1UL);
  for( ulong i=0UL; i<gui->vote_account.vote_account_cnt; i++ ) {
    int found = 0;
    for( ulong j=0UL; j<peer_cnt; j++ ) {
      if( FD_UNLIKELY( !memcmp( gui->vote_account.vote_accounts[ i ].vote_account, data+j*112UL, 32UL ) ) ) {
        found = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found ) ) {
      fd_memcpy( removed[ removed_cnt++ ].uc, gui->vote_account.vote_accounts[ i ].vote_account->uc, 32UL );
      if( FD_LIKELY( i+1UL!=gui->vote_account.vote_account_cnt ) ) {
        fd_memcpy( &gui->vote_account.vote_accounts[ i ], &gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt-1UL ], sizeof(struct fd_gui_vote_account) );
        gui->vote_account.vote_account_cnt--;
        i--;
      }
    }
  }

  ulong before_peer_cnt = gui->vote_account.vote_account_cnt;
  for( ulong i=0UL; i<peer_cnt; i++ ) {
    int found = 0;
    ulong found_idx;
    for( ulong j=0UL; j<gui->vote_account.vote_account_cnt; j++ ) {
      if( FD_UNLIKELY( !memcmp( gui->vote_account.vote_accounts[ j ].vote_account, data+i*112UL, 32UL ) ) ) {
        found_idx = j;
        found = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found ) ) {
      fd_memcpy( gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].vote_account->uc, data+i*112UL, 32UL );
      fd_memcpy( gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].pubkey->uc, data+i*112UL+32UL, 32UL );

      gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].activated_stake = *(ulong const *)(data+i*112UL+64UL);
      gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].last_vote = *(ulong const *)(data+i*112UL+72UL);
      gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].root_slot = *(ulong const *)(data+i*112UL+80UL);
      gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].epoch_credits = *(ulong const *)(data+i*112UL+88UL);
      gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].commission = *(data+i*112UL+96UL);
      gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].delinquent = *(data+i*112UL+97UL);

      gui->vote_account.vote_account_cnt++;
    } else {
      int peer_updated =
        memcmp( gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].pubkey->uc, data+i*112UL+32UL, 32UL ) ||
        gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].activated_stake != *(ulong const *)(data+i*112UL+64UL) ||
        gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].last_vote       != *(ulong const *)(data+i*112UL+72UL) ||
        gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].root_slot       != *(ulong const *)(data+i*112UL+80UL) ||
        gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].epoch_credits   != *(ulong const *)(data+i*112UL+88UL) ||
        gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].commission      != *(data+i*112UL+96UL) ||
        gui->vote_account.vote_accounts[ gui->vote_account.vote_account_cnt ].delinquent      != *(data+i*112UL+97UL);

      if( FD_UNLIKELY( peer_updated ) ) {
        updated[ update_cnt++ ] = found_idx;

        fd_memcpy( gui->vote_account.vote_accounts[ found_idx ].pubkey->uc, data+i*112UL+32UL, 32UL );
        gui->vote_account.vote_accounts[ found_idx ].activated_stake = *(ulong const *)(data+i*112UL+64UL);
        gui->vote_account.vote_accounts[ found_idx ].last_vote = *(ulong const *)(data+i*112UL+72UL);
        gui->vote_account.vote_accounts[ found_idx ].root_slot = *(ulong const *)(data+i*112UL+80UL);
        gui->vote_account.vote_accounts[ found_idx ].epoch_credits = *(ulong const *)(data+i*112UL+88UL);
        gui->vote_account.vote_accounts[ found_idx ].commission = *(data+i*112UL+96UL);
        gui->vote_account.vote_accounts[ found_idx ].delinquent = *(data+i*112UL+97UL);
      }
    }
  }

  added_cnt = gui->vote_account.vote_account_cnt - before_peer_cnt;
  for( ulong i=before_peer_cnt; i<gui->vote_account.vote_account_cnt; i++ ) added[ i-before_peer_cnt ] = i;

  fd_gui_printf_peers_vote_account_update( gui, updated, update_cnt, removed, removed_cnt, added, added_cnt );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

static void
fd_gui_handle_validator_info_update( fd_gui_t *    gui,
                                     uchar const * msg ) {
  ulong const * header = (ulong const *)fd_type_pun_const( msg );
  ulong peer_cnt = header[ 0 ];

  ulong added_cnt = 0UL;
  ulong added[ 40200 ] = {0};

  ulong update_cnt = 0UL;
  ulong updated[ 40200 ] = {0};

  ulong removed_cnt = 0UL;
  fd_pubkey_t removed[ 40200 ] = {0};

  uchar const * data = (uchar const *)(header+1UL);
  for( ulong i=0UL; i<gui->validator_info.info_cnt; i++ ) {
    int found = 0;
    for( ulong j=0UL; j<peer_cnt; j++ ) {
      if( FD_UNLIKELY( !memcmp( gui->validator_info.info[ i ].pubkey, data+j*608UL, 32UL ) ) ) {
        found = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found ) ) {
      fd_memcpy( removed[ removed_cnt++ ].uc, gui->validator_info.info[ i ].pubkey->uc, 32UL );
      if( FD_LIKELY( i+1UL!=gui->validator_info.info_cnt ) ) {
        fd_memcpy( &gui->validator_info.info[ i ], &gui->validator_info.info[ gui->validator_info.info_cnt-1UL ], sizeof(struct fd_gui_validator_info) );
        gui->validator_info.info_cnt--;
        i--;
      }
    }
  }

  ulong before_peer_cnt = gui->validator_info.info_cnt;
  for( ulong i=0UL; i<peer_cnt; i++ ) {
    int found = 0;
    ulong found_idx;
    for( ulong j=0UL; j<gui->validator_info.info_cnt; j++ ) {
      if( FD_UNLIKELY( !memcmp( gui->validator_info.info[ j ].pubkey, data+i*608UL, 32UL ) ) ) {
        found_idx = j;
        found = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found ) ) {
      fd_memcpy( gui->validator_info.info[ gui->validator_info.info_cnt ].pubkey->uc, data+i*608UL, 32UL );

      strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].name, (char const *)(data+i*608UL+32UL), 64 );
      gui->validator_info.info[ gui->validator_info.info_cnt ].name[ 63 ] = '\0';

      strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].website, (char const *)(data+i*608UL+96UL), 128 );
      gui->validator_info.info[ gui->validator_info.info_cnt ].website[ 127 ] = '\0';

      strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].details, (char const *)(data+i*608UL+224UL), 256 );
      gui->validator_info.info[ gui->validator_info.info_cnt ].details[ 255 ] = '\0';

      strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].icon_uri, (char const *)(data+i*608UL+480UL), 128 );
      gui->validator_info.info[ gui->validator_info.info_cnt ].icon_uri[ 127 ] = '\0';

      gui->validator_info.info_cnt++;
    } else {
      int peer_updated =
        memcmp( gui->validator_info.info[ gui->validator_info.info_cnt ].pubkey->uc, data+i*608UL, 32UL ) ||
        strncmp( gui->validator_info.info[ gui->validator_info.info_cnt ].name, (char const *)(data+i*608UL+32UL), 64 ) ||
        strncmp( gui->validator_info.info[ gui->validator_info.info_cnt ].website, (char const *)(data+i*608UL+96UL), 128 ) ||
        strncmp( gui->validator_info.info[ gui->validator_info.info_cnt ].details, (char const *)(data+i*608UL+224UL), 256 ) ||
        strncmp( gui->validator_info.info[ gui->validator_info.info_cnt ].icon_uri, (char const *)(data+i*608UL+480UL), 128 );

      if( FD_UNLIKELY( peer_updated ) ) {
        updated[ update_cnt++ ] = found_idx;

        fd_memcpy( gui->validator_info.info[ gui->validator_info.info_cnt ].pubkey->uc, data+i*608UL, 32UL );

        strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].name, (char const *)(data+i*608UL+32UL), 64 );
        gui->validator_info.info[ gui->validator_info.info_cnt ].name[ 63 ] = '\0';

        strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].website, (char const *)(data+i*608UL+96UL), 128 );
        gui->validator_info.info[ gui->validator_info.info_cnt ].website[ 127 ] = '\0';

        strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].details, (char const *)(data+i*608UL+224UL), 256 );
        gui->validator_info.info[ gui->validator_info.info_cnt ].details[ 255 ] = '\0';

        strncpy( gui->validator_info.info[ gui->validator_info.info_cnt ].icon_uri, (char const *)(data+i*608UL+480UL), 128 );
        gui->validator_info.info[ gui->validator_info.info_cnt ].icon_uri[ 127 ] = '\0';
      }
    }
  }

  added_cnt = gui->validator_info.info_cnt - before_peer_cnt;
  for( ulong i=before_peer_cnt; i<gui->validator_info.info_cnt; i++ ) added[ i-before_peer_cnt ] = i;

  fd_gui_printf_peers_validator_info_update( gui, updated, update_cnt, removed, removed_cnt, added, added_cnt );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

int
fd_gui_request_slot( fd_gui_t *    gui,
                     ulong         ws_conn_id,
                     ulong         request_id,
                     cJSON const * params ) {
  const cJSON * slot_param = cJSON_GetObjectItemCaseSensitive( params, "slot" );
  if( FD_UNLIKELY( !cJSON_IsNumber( slot_param ) ) ) return FD_HTTP_SERVER_CONNECTION_CLOSE_BAD_REQUEST;

  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);

  ulong _slot = slot_param->valueulong;
  fd_gui_slot_t const * slot = gui->slots[ _slot % slots_sz ];
  if( FD_UNLIKELY( slot->slot!=_slot ) ) {
    fd_gui_printf_null_query_response( gui, "slot", "query", request_id );
    FD_TEST( !fd_hcache_snap_ws_send( gui->hcache, ws_conn_id ) );
    return 0;
  }

  fd_gui_printf_slot_request( gui, _slot, request_id );
  FD_TEST( !fd_hcache_snap_ws_send( gui->hcache, ws_conn_id ) );
  return 0;
}

int
fd_gui_ws_message( fd_gui_t *    gui,
                   ulong         ws_conn_id,
                   uchar const * data,
                   ulong         data_len ) {
  /* TODO: cJSON allocates, might fail SIGSYS due to brk(2)...
     switch off this (or use wksp allocator) */
  const char * parse_end;
  cJSON * json = cJSON_ParseWithLengthOpts( (char *)data, data_len, &parse_end, 0 );
  if( FD_UNLIKELY( !json ) ) {
    return FD_HTTP_SERVER_CONNECTION_CLOSE_BAD_REQUEST;
  }

  const cJSON * node = cJSON_GetObjectItemCaseSensitive( json, "id" );
  if( FD_UNLIKELY( !cJSON_IsNumber( node ) ) ) {
    cJSON_Delete( json );
    return FD_HTTP_SERVER_CONNECTION_CLOSE_BAD_REQUEST;
  }
  ulong id = node->valueulong;

  const cJSON * topic = cJSON_GetObjectItemCaseSensitive( json, "topic" );
  if( FD_UNLIKELY( !cJSON_IsString( topic ) || topic->valuestring==NULL ) ) {
    cJSON_Delete( json );
    return FD_HTTP_SERVER_CONNECTION_CLOSE_BAD_REQUEST;
  }

  const cJSON * key = cJSON_GetObjectItemCaseSensitive( json, "key" );
  if( FD_UNLIKELY( !cJSON_IsString( key ) || key->valuestring==NULL ) ) {
    cJSON_Delete( json );
    return FD_HTTP_SERVER_CONNECTION_CLOSE_BAD_REQUEST;
  }

  if( FD_LIKELY( !strcmp( topic->valuestring, "slot" ) && !strcmp( key->valuestring, "query" ) ) ) {
    const cJSON * params = cJSON_GetObjectItemCaseSensitive( json, "params" );
    if( FD_UNLIKELY( !cJSON_IsObject( params ) ) ) {
      cJSON_Delete( json );
      return FD_HTTP_SERVER_CONNECTION_CLOSE_BAD_REQUEST;
    }

    int result = fd_gui_request_slot( gui, ws_conn_id, id, params );
    cJSON_Delete( json );
    return result;
  }

  cJSON_Delete( json );
  return FD_HTTP_SERVER_CONNECTION_CLOSE_UNKNOWN_METHOD;
}

static void
fd_gui_clear_slot( fd_gui_t * gui,
                   ulong      _slot ) {
  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);
  fd_gui_slot_t * slot = gui->slots[ _slot % slots_sz ];

  int mine = 0;
  for( ulong i=0UL; i<2UL; i++) {
    if( FD_LIKELY( _slot>=gui->epoch.epochs[ i ].start_slot && _slot<=gui->epoch.epochs[ i ].end_slot ) ) {
      fd_pubkey_t const * slot_leader = fd_epoch_leaders_get( gui->epoch.epochs[ i ].lsched, _slot );
      mine = !memcmp( slot_leader->uc, gui->summary.identity_key->uc, 32UL );
      break;
    }
  }

  slot->slot           = _slot;
  slot->mine           = mine;
  slot->skipped        = 1;
  slot->level          = FD_GUI_SLOT_LEVEL_INCOMPLETE;
  slot->total_txn_cnt  = ULONG_MAX;
  slot->vote_txn_cnt   = ULONG_MAX;
  slot->failed_txn_cnt = ULONG_MAX;
  slot->compute_units  = ULONG_MAX;
  slot->fees           = ULONG_MAX;
}

static void
fd_gui_handle_leader_schedule( fd_gui_t *    gui,
                               ulong const * msg ) {
  ulong epoch               = msg[ 0 ];
  ulong staked_cnt          = msg[ 1 ];
  ulong start_slot          = msg[ 2 ];
  ulong slot_cnt            = msg[ 3 ];
  ulong excluded_stake      = msg[ 4 ];

  FD_TEST( staked_cnt<=50000UL );

  ulong idx = epoch % 2UL;
  gui->epoch.has_epoch[ idx ] = 1;

  gui->epoch.epochs[ idx ].epoch          = epoch;
  gui->epoch.epochs[ idx ].start_slot     = start_slot;
  gui->epoch.epochs[ idx ].end_slot       = start_slot + slot_cnt - 1; // end_slot is inclusive.
  gui->epoch.epochs[ idx ].excluded_stake = excluded_stake;
  fd_epoch_leaders_delete( fd_epoch_leaders_leave( gui->epoch.epochs[ idx ].lsched ) );
  gui->epoch.epochs[idx].lsched = fd_epoch_leaders_join( fd_epoch_leaders_new( gui->epoch.epochs[ idx ]._lsched,
                                                                                epoch,
                                                                                gui->epoch.epochs[ idx ].start_slot,
                                                                                slot_cnt,
                                                                                staked_cnt,
                                                                                fd_type_pun_const( msg+5UL ),
                                                                                excluded_stake ) );
  fd_memcpy( gui->epoch.epochs[ idx ].stakes, fd_type_pun_const( msg+5UL ), staked_cnt*sizeof(gui->epoch.epochs[ idx ].stakes[ 0 ]) );

  fd_gui_printf_epoch( gui, idx );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

static void
fd_gui_handle_slot_start( fd_gui_t * gui,
                          ulong *    _slot ) {
  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);
  fd_gui_slot_t * slot = gui->slots[ _slot[ 0 ] % slots_sz ];

  if( FD_UNLIKELY( slot->slot!=_slot[ 0 ] ) ) fd_gui_clear_slot( gui, _slot[ 0 ] );
  fd_gui_tile_timers_snap( gui, slot->tile_timers_begin );
  slot->tile_timers_begin_snap_idx = gui->summary.tile_timers_snap_idx;
}

static void
fd_gui_handle_slot_end( fd_gui_t * gui,
                        ulong *    _slot ) {
  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);
  fd_gui_slot_t * slot = gui->slots[ _slot[ 0 ] % slots_sz ];

  if( FD_UNLIKELY( slot->slot!=_slot[ 0 ] ) ) fd_gui_clear_slot( gui, _slot[ 0 ] );
  fd_gui_tile_timers_snap( gui, slot->tile_timers_end );
  slot->tile_timers_end_snap_idx = gui->summary.tile_timers_snap_idx;

  /* When a slot ends, snap the state of the waterfall and save it into
     that slot, and also reset the reference counters to the end of the
     slot. */

  fd_gui_txn_waterfall_snap( gui, slot->waterfall_end );
  memcpy( gui->summary.txn_waterfall_reference, slot->waterfall_end, sizeof(gui->summary.txn_waterfall_reference) );
  memcpy( gui->summary.txn_waterfall_current,   slot->waterfall_end, sizeof(gui->summary.txn_waterfall_current) );
}

static void
fd_gui_handle_reset_slot( fd_gui_t * gui,
                          ulong *    msg ) {
  ulong parent_cnt = msg[ 0 ];

  ulong _slot = msg[ 1 ];
  ulong parent_slot_idx = 0UL;

  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);
  for( ulong i=0UL; i<=_slot; i++ ) {
    fd_gui_slot_t * slot = gui->slots[ (_slot-i) % slots_sz ];

    int should_republish = 0;
    if( FD_UNLIKELY( slot->slot==ULONG_MAX || slot->slot!=(_slot-i) ) ) {
      fd_gui_clear_slot( gui, (_slot-i) );
      should_republish = 1;
    }

    /* The chain of parents may stretch into already rooted slots if
       they haven't been squashed yet, if we reach one of them we can
       just exit, all the information prior to the root is already
       correct. */

    if( FD_LIKELY( slot->level>=FD_GUI_SLOT_LEVEL_ROOTED ) ) break;

    if( FD_UNLIKELY( (_slot-i)!=msg[1UL+parent_slot_idx] ) ) {
      /* We are between two parents in the rooted chain, which means
         we were skipped. */
      if( FD_UNLIKELY( !slot->skipped ) ) {
        slot->skipped = 1;
        should_republish = 1;
      }
    } else {
      /* Reached the next parent... */
      if( FD_UNLIKELY( slot->skipped ) ) {
        slot->skipped = 0;
        should_republish = 1;
      }
      parent_slot_idx++;
    }

    if( FD_LIKELY( should_republish ) ) {
      fd_gui_printf_slot( gui, _slot );
      fd_hcache_snap_ws_broadcast( gui->hcache );
    }

    /* We reached the last parent in the chain, everything above this
       must have already been rooted, so we can exit. */

    if( FD_UNLIKELY( parent_slot_idx>=parent_cnt ) ) break;
  }

  ulong total_txn_cnt  = 0UL;
  ulong vote_txn_cnt   = 0UL;
  ulong failed_txn_cnt = 0UL;

  ulong last_total_txn_cnt  = 0UL;
  ulong last_vote_txn_cnt   = 0UL;
  ulong last_failed_txn_cnt = 0UL;
  long  last_time_nanos     = 0L;

  for( ulong i=0UL; i<=fd_ulong_min(_slot, 150UL); i++ ) {
    ulong parent = (_slot+(slots_sz-i))%slots_sz;

    fd_gui_slot_t * slot = gui->slots[ parent ];
    if( FD_UNLIKELY( slot->slot==ULONG_MAX) ) break;

    FD_TEST( slot->slot==(_slot-i) );

    if( FD_LIKELY( !slot->skipped ) ) {
      total_txn_cnt  += slot->total_txn_cnt;
      vote_txn_cnt   += slot->vote_txn_cnt;
      failed_txn_cnt += slot->failed_txn_cnt;

      last_total_txn_cnt  = slot->total_txn_cnt;
      last_vote_txn_cnt   = slot->vote_txn_cnt;
      last_failed_txn_cnt = slot->failed_txn_cnt;
      last_time_nanos     = slot->completed_time;
    }
  }

  total_txn_cnt  -= last_total_txn_cnt;
  vote_txn_cnt   -= last_vote_txn_cnt;
  failed_txn_cnt -= last_failed_txn_cnt;

  long now = fd_log_wallclock();
  gui->summary.estimated_tps        = (total_txn_cnt *1000000000UL)/(ulong)(now-last_time_nanos);
  gui->summary.estimated_vote_tps   = (vote_txn_cnt  *1000000000UL)/(ulong)(now-last_time_nanos);
  gui->summary.estimated_failed_tps = (failed_txn_cnt*1000000000UL)/(ulong)(now-last_time_nanos);

  fd_gui_printf_estimated_tps( gui );
  fd_hcache_snap_ws_broadcast( gui->hcache );
  fd_gui_printf_estimated_vote_tps( gui );
  fd_hcache_snap_ws_broadcast( gui->hcache );
  fd_gui_printf_estimated_nonvote_tps( gui );
  fd_hcache_snap_ws_broadcast( gui->hcache );
  fd_gui_printf_estimated_failed_tps( gui );
  fd_hcache_snap_ws_broadcast( gui->hcache );

  ulong last_slot = _slot;
  long last_published = gui->slots[ _slot % slots_sz ]->completed_time;

  for( ulong i=0UL; i<fd_ulong_min( _slot, 750UL ); i++ ) {
    ulong parent = _slot-i;

    fd_gui_slot_t * slot = gui->slots[ parent % slots_sz ];
    if( FD_UNLIKELY( slot->slot==ULONG_MAX) ) break;
    FD_TEST( slot->slot==parent );

    last_slot      = parent;
    last_published = slot->completed_time;
  }

  if( FD_LIKELY( _slot!=last_slot )) {
    fd_gui_slot_t * parent_slot = gui->slots[ last_slot % slots_sz ];

    gui->summary.estimated_slot_duration_nanos = (ulong)(now-last_published)/(_slot-last_slot);
    fd_gui_printf_estimated_slot_duration_nanos( gui );
    fd_hcache_snap_ws_broadcast( gui->hcache );
  }

  if( FD_LIKELY( _slot>gui->summary.slot_completed ) ) {
    gui->summary.slot_completed = _slot;
    fd_gui_printf_completed_slot( gui );
    FD_TEST( !fd_hcache_snap_ws_broadcast( gui->hcache ) );
  }
}

static void
fd_gui_handle_completed_slot( fd_gui_t * gui,
                              ulong *    msg ) {
  ulong _slot = msg[ 0 ];
  ulong total_txn_count = msg[ 1 ];
  ulong nonvote_txn_count = msg[ 2 ];
  ulong failed_txn_count = msg[ 3 ];
  ulong compute_units = msg[ 4 ];
  ulong fees = msg[ 5 ];

  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);
  fd_gui_slot_t * slot = gui->slots[ _slot % slots_sz ];
  if( FD_UNLIKELY( slot->slot!=_slot ) ) fd_gui_clear_slot( gui, _slot );

  slot->completed_time = fd_log_wallclock();
  FD_TEST( slot->level<FD_GUI_SLOT_LEVEL_COMPLETED );
  slot->level          = FD_GUI_SLOT_LEVEL_COMPLETED;
  slot->total_txn_cnt  = total_txn_count;
  slot->vote_txn_cnt   = total_txn_count - nonvote_txn_count;
  slot->failed_txn_cnt = failed_txn_count;
  slot->compute_units  = compute_units;
  slot->fees           = fees;

  fd_gui_printf_slot( gui, _slot );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

static void
fd_gui_handle_rooted_slot( fd_gui_t * gui,
                           ulong *    msg ) {
  ulong _slot = msg[ 0 ];

  ulong slots_sz = sizeof(gui->slots) / sizeof(gui->slots[ 0 ]);
  for( ulong i=0UL; i<=_slot; i++ ) {
    fd_gui_slot_t * slot = gui->slots[ (_slot-i) % slots_sz ];

    FD_TEST( slot->slot==(_slot-i) );
    if( FD_UNLIKELY( slot->level>=FD_GUI_SLOT_LEVEL_ROOTED ) ) break;

    slot->level = FD_GUI_SLOT_LEVEL_ROOTED;
    fd_gui_printf_slot( gui, _slot );
    fd_hcache_snap_ws_broadcast( gui->hcache );
  }

  gui->summary.slot_rooted = _slot;
  fd_gui_printf_root_slot( gui );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

static void
fd_gui_handle_optimistically_confirmed_slot( fd_gui_t * gui,
                                             ulong *    msg ) {
  gui->summary.slot_optimistically_confirmed = msg[ 0];
  fd_gui_printf_optimistically_confirmed_slot( gui );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

static void
fd_gui_handle_balance_update( fd_gui_t * gui,
                              ulong      balance ) {
  gui->summary.balance = balance;
  fd_gui_printf_balance( gui );
  fd_hcache_snap_ws_broadcast( gui->hcache );
}

void
fd_gui_plugin_message( fd_gui_t *    gui,
                       ulong         plugin_msg,
                       uchar const * msg,
                       ulong         msg_len ) {
  (void)msg_len;

  switch( plugin_msg ) {
    case FD_PLUGIN_MSG_SLOT_ROOTED:
      fd_gui_handle_rooted_slot( gui, (ulong *)msg );
      break;
    case FD_PLUGIN_MSG_SLOT_OPTIMISTICALLY_CONFIRMED:
      fd_gui_handle_optimistically_confirmed_slot( gui, (ulong *)msg );
      break;
    case FD_PLUGIN_MSG_SLOT_COMPLETED:
      fd_gui_handle_completed_slot( gui, (ulong *)msg );
      break;
    case FD_PLUGIN_MSG_SLOT_ESTIMATED:
      gui->summary.slot_estimated = *(ulong const *)msg;
      fd_gui_printf_estimated_slot( gui );
      fd_hcache_snap_ws_broadcast( gui->hcache );
      break;
    case FD_PLUGIN_MSG_LEADER_SCHEDULE: {
      fd_gui_handle_leader_schedule( gui, (ulong const *)msg );
      break;
    }
    case FD_PLUGIN_MSG_SLOT_START: {
      fd_gui_handle_slot_start( gui, (ulong *)msg );
      break;
    }
    case FD_PLUGIN_MSG_SLOT_END: {
      fd_gui_handle_slot_end( gui, (ulong *)msg );
      break;
    }
    case FD_PLUGIN_MSG_GOSSIP_UPDATE: {
      fd_gui_handle_gossip_update( gui, msg );
      break;
    }
    case FD_PLUGIN_MSG_VOTE_ACCOUNT_UPDATE: {
      fd_gui_handle_vote_account_update( gui, msg );
      break;
    }
    case FD_PLUGIN_MSG_VALIDATOR_INFO: {
      fd_gui_handle_validator_info_update( gui, msg );
      break;
    }
    case FD_PLUGIN_MSG_SLOT_RESET: {
      fd_gui_handle_reset_slot( gui, (ulong *)msg );
      break;
    }
    case FD_PLUGIN_MSG_BALANCE: {
      fd_gui_handle_balance_update( gui, *(ulong *)msg );
      break;
    }
    default:
      FD_LOG_ERR(( "Unhandled plugin msg: 0x%lx", plugin_msg ));
      break;
  }
}
