#include <stdio.h>
#include "../fd_banks_solana.h"
#include "../fd_acc_mgr.h"
#include "../fd_hashes.h"
#include "fd_sysvar.h"
#include "../fd_runtime.h"

#ifdef _DISABLE_OPTIMIZATION
#pragma GCC optimize ("O0")
#endif

// run --ledger /home/jsiegel/test-ledger --db /home/jsiegel/funk --cmd accounts --accounts /home/jsiegel/test-ledger/accounts/ --pages 15 --index-max 120000000 --start-slot 2 --end-slot 2 --start-id 35 --end-id 37
// run --ledger /home/jsiegel/test-ledger --db /home/jsiegel/funk --cmd replay --pages 15 --index-max 120000000 --start-slot 0 --end-slot 3

// {meta = {write_version_obsolete = 137, 
// data_len = 6008, pubkey = "\006\247\325\027\031,V\216\340\212\204_sҗ\210\317\003\\1E\262\032\263D\330\006.\251@\000"}, info = {lamports = 42706560, rent_epoch = 0, owner = "\006\247\325\027\030u\367)\307=\223@\217!a \006~،v\340\214(\177\301\224`\000\000\000", executable = 0 '\000', padding = "K\000\f\376\177\000"}, hash = {value = "\302Q\316\035qTY\347\352]\260\335\213\224R\227ԯ\366R\273\063H\345֑c\377\207/k\275"}}

// owner:      Sysvar1111111111111111111111111111111111111 pubkey:      SysvarRecentB1ockHashes11111111111111111111 hash:     E5YSehyvJ7xXcNnQjWCH9UhMJ1dxDBJ1RuuPh1Y3RZgg file: /home/jsiegel/test-ledger/accounts//2.37
//   {blockhash = JCidNXtcMXMWQwMDM3ZQq5pxaw3hQpNbeHg1KcstjuF4,  fee_calculator={lamports_per_signature = 5000}}
//   {blockhash = GQN3oV8G1Ra3GCX76dE1YYJ6UjMyDreNCEWM4tZ39zj1,  fee_calculator={lamports_per_signature = 5000}}
//   {blockhash = Ha5DVgnD1xSA8oQc337jtA3atEfQ4TFX1ajeZG1Y2tUx,  fee_calculator={lamports_per_signature = 0}}

void fd_sysvar_recent_hashes_init( fd_global_ctx_t* global ) {
  // https://github.com/solana-labs/solana/blob/8f2c8b8388a495d2728909e30460aa40dcc5d733/sdk/program/src/fee_calculator.rs#L110

  if (global->bank.solana_bank.slot != 0) 
    return;

  fd_block_block_hash_entry_t * hashes = global->bank.recent_block_hashes.hashes =
    deq_fd_block_block_hash_entry_t_alloc( global->allocf, global->allocf_arg );
  fd_block_block_hash_entry_t * elem = deq_fd_block_block_hash_entry_t_push_head_nocopy(hashes);
  fd_block_block_hash_entry_new(elem);
  fd_memcpy(elem->blockhash.hash, global->genesis_hash, sizeof(global->genesis_hash));

  ulong sz = fd_recent_block_hashes_size(&global->bank.recent_block_hashes);
  if (sz < 6008)
    sz = 6008;
  unsigned char *enc = fd_alloca(1, sz);
  memset(enc, 0, sz);
  fd_bincode_encode_ctx_t ctx;
  ctx.data = enc;
  ctx.dataend = enc + sz;
  if ( fd_recent_block_hashes_encode(&global->bank.recent_block_hashes, &ctx) )
    FD_LOG_ERR(("fd_recent_block_hashes_encode failed"));

  fd_sysvar_set(global, global->sysvar_owner, global->sysvar_recent_block_hashes, enc, sz, global->bank.solana_bank.slot );
}

void fd_sysvar_recent_hashes_update(fd_global_ctx_t* global ) {
  if (global->bank.solana_bank.slot == 0)  // we already set this... as part of boot
    return; 

  fd_block_block_hash_entry_t * hashes = global->bank.recent_block_hashes.hashes;
  fd_bincode_destroy_ctx_t ctx2;
  ctx2.freef = global->freef;
  ctx2.freef_arg = global->allocf_arg;
  while (deq_fd_block_block_hash_entry_t_cnt(hashes) >= 150)
    fd_block_block_hash_entry_destroy( deq_fd_block_block_hash_entry_t_pop_tail_nocopy( hashes ), &ctx2 );

  FD_TEST( !deq_fd_block_block_hash_entry_t_full(hashes) );
  fd_block_block_hash_entry_t * elem = deq_fd_block_block_hash_entry_t_push_head_nocopy(hashes);
  fd_block_block_hash_entry_new(elem);
  fd_memcpy(elem->blockhash.hash, global->block_hash, sizeof(global->block_hash));
  elem->fee_calculator.lamports_per_signature = fd_runtime_txn_lamports_per_signature(global, NULL, NULL);

  ulong sz = fd_recent_block_hashes_size(&global->bank.recent_block_hashes);
  if (sz < 6008)
    sz = 6008;
  unsigned char *enc = fd_alloca(1, sz);
  memset(enc, 0, sz);
  fd_bincode_encode_ctx_t ctx;
  ctx.data = enc;
  ctx.dataend = enc + sz;
  if ( fd_recent_block_hashes_encode(&global->bank.recent_block_hashes, &ctx) )
    FD_LOG_ERR(("fd_recent_block_hashes_encode failed"));

  fd_sysvar_set(global, global->sysvar_owner, global->sysvar_recent_block_hashes, enc, sz, global->bank.solana_bank.slot);
}
