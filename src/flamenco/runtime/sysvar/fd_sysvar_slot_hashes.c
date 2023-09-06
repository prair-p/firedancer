#include "fd_sysvar_slot_hashes.h"
#include "../../../flamenco/types/fd_types.h"
#include "fd_sysvar.h"

/* https://github.com/solana-labs/solana/blob/8f2c8b8388a495d2728909e30460aa40dcc5d733/sdk/program/src/slot_hashes.rs#L11 */
const ulong slot_hashes_max_entries = 512;

/* https://github.com/solana-labs/solana/blob/8f2c8b8388a495d2728909e30460aa40dcc5d733/sdk/program/src/sysvar/slot_hashes.rs#L12 */
const ulong slot_hashes_min_account_size = 20488;

void write_slot_hashes( fd_global_ctx_t* global, fd_slot_hashes_t* slot_hashes ) {
  ulong sz = fd_slot_hashes_size( slot_hashes );
  if (sz < slot_hashes_min_account_size)
    sz = slot_hashes_min_account_size;
  unsigned char *enc = fd_alloca( 1, sz );
  memset( enc, 0, sz );
  fd_bincode_encode_ctx_t ctx;
  ctx.data = enc;
  ctx.dataend = enc + sz;
  if ( fd_slot_hashes_encode( slot_hashes, &ctx ) )
    FD_LOG_ERR(("fd_slot_hashes_encode failed"));

  fd_sysvar_set( global, global->sysvar_owner, (fd_pubkey_t *) global->sysvar_slot_hashes, enc, sz, global->bank.slot, NULL );
}

//void fd_sysvar_slot_hashes_init( fd_global_ctx_t* global ) {
//  fd_slot_hashes_t slot_hashes;
//  memset( &slot_hashes, 0, sizeof(fd_slot_hashes_t) );
//  write_slot_hashes( global, &slot_hashes );
//}

/* https://github.com/solana-labs/solana/blob/8f2c8b8388a495d2728909e30460aa40dcc5d733/sdk/program/src/slot_hashes.rs#L34 */
void fd_sysvar_slot_hashes_update( fd_global_ctx_t* global ) {
  FD_SCRATCH_SCOPED_FRAME;

  fd_slot_hashes_t slot_hashes;
  int err = fd_sysvar_slot_hashes_read( global, &slot_hashes );
  switch( err ) {
  case 0: break;
  case FD_ACC_MGR_ERR_UNKNOWN_ACCOUNT:
    slot_hashes.hashes = deq_fd_slot_hash_t_alloc( global->valloc );
    FD_TEST( slot_hashes.hashes );
    break;
  default:
    FD_LOG_ERR(( "fd_sysvar_slot_hashes_read failed (%d)", err ));
  }

  fd_slot_hash_t * hashes = slot_hashes.hashes;

  uchar found = 0;
  for ( deq_fd_slot_hash_t_iter_t iter = deq_fd_slot_hash_t_iter_init( hashes );
        !deq_fd_slot_hash_t_iter_done( hashes, iter );
        iter = deq_fd_slot_hash_t_iter_next( hashes, iter ) ) {
    fd_slot_hash_t * ele = deq_fd_slot_hash_t_iter_ele( hashes, iter );
    if ( ele->slot == global->bank.slot ) {
      memcpy( &ele->hash, &global->bank.banks_hash, sizeof(fd_hash_t) );
      found = 1;
    }
  }

  if ( !found ) {
  // https://github.com/firedancer-io/solana/blob/08a1ef5d785fe58af442b791df6c4e83fe2e7c74/runtime/src/bank.rs#L2371
    fd_slot_hash_t slot_hash = {
      .hash = global->bank.banks_hash, // parent hash?
      .slot = global->bank.slot - 1,   // parent_slot
    };

    if (FD_UNLIKELY(global->log_level > 2))  {
      FD_LOG_WARNING(( "fd_sysvar_slot_hash_update:  slot %ld,  hash %32J", slot_hash.slot, slot_hash.hash.key ));
    }

    fd_bincode_destroy_ctx_t ctx2 = { .valloc = global->valloc };

    if (deq_fd_slot_hash_t_full( hashes ) )
      fd_slot_hash_destroy( deq_fd_slot_hash_t_pop_tail_nocopy( hashes ), &ctx2 );

    deq_fd_slot_hash_t_push_head( hashes, slot_hash );
  }

  write_slot_hashes( global, &slot_hashes );
  fd_bincode_destroy_ctx_t ctx = { .valloc = global->valloc };
  fd_slot_hashes_destroy( &slot_hashes, &ctx );
}

int
fd_sysvar_slot_hashes_read( fd_global_ctx_t *  global,
                            fd_slot_hashes_t * result ) {

//  FD_LOG_INFO(( "SysvarS1otHashes111111111111111111111111111 at slot %lu: " FD_LOG_HEX16_FMT, global->bank.slot, FD_LOG_HEX16_FMT_ARGS(     metadata.hash    ) ));

  int err = 0;
  uchar const * raw_acc_data = fd_acc_mgr_view_raw( global->acc_mgr, global->funk_txn, (fd_pubkey_t const *)global->sysvar_slot_hashes, NULL, &err );
  if (FD_UNLIKELY(!FD_RAW_ACCOUNT_EXISTS(raw_acc_data))) return err;

  fd_account_meta_t const * metadata = (fd_account_meta_t const *)raw_acc_data;
  uchar const *             data     = raw_acc_data + metadata->hlen;

  fd_bincode_decode_ctx_t decode = {
    .data    = data,
    .dataend = data + metadata->dlen,
    .valloc  = global->valloc /* !!! There is no reason to place this on the global heap.  Use scratch instead. */
  };
  err = fd_slot_hashes_decode( result, &decode );
  if( FD_UNLIKELY( err!=FD_BINCODE_SUCCESS ) ) return err;

  return 0;
}
