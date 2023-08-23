#include "fd_bpf_upgradeable_loader_program.h"

#include "../fd_account.h"
#include "../../../ballet/base58/fd_base58.h"
#include "../sysvar/fd_sysvar_rent.h"
#include "../../../ballet/sbpf/fd_sbpf_loader.h"
#include "../../../ballet/sbpf/fd_sbpf_maps.h"
#include "../../vm/fd_vm_syscalls.h"
#include "../../vm/fd_vm_interp.h"
#include "../../vm/fd_vm_disasm.h"

#include <stdio.h>

#define BUFFER_METADATA_SIZE  (37)
#define PROGRAMDATA_METADATA_SIZE (45UL)
#define MAX_PERMITTED_DATA_INCREASE (10 * 1024)
#define SIZE_OF_PROGRAM (36)

char *
read_bpf_upgradeable_loader_state( fd_global_ctx_t* global, fd_pubkey_t* program_acc, fd_bpf_upgradeable_loader_state_t * result, int *opt_err) {
  int err = 0;
  char * raw_acc_data = (char*) fd_acc_mgr_view_raw(global->acc_mgr, global->funk_txn, (fd_pubkey_t *) program_acc, NULL, &err);
  if (NULL == raw_acc_data) {
    if (NULL != opt_err)
      *opt_err = err;
    return NULL;
  }
  fd_account_meta_t *m = (fd_account_meta_t *) raw_acc_data;

  fd_bincode_decode_ctx_t ctx = {
    .data = raw_acc_data + m->hlen,
    .dataend = (char *) ctx.data + m->dlen,
    .valloc  = global->valloc,
  };

  fd_bpf_upgradeable_loader_state_new(result);

  if ( fd_bpf_upgradeable_loader_state_decode( result, &ctx ) ) {
    FD_LOG_DEBUG(("fd_bpf_upgradeable_loader_state_decode failed"));
    return NULL;
  }

  return raw_acc_data;
}

int write_bpf_upgradeable_loader_state(fd_global_ctx_t* global, fd_pubkey_t* program_acc, fd_bpf_upgradeable_loader_state_t * loader_state) {
  int err = 0;
  ulong encoded_loader_state_size = fd_bpf_upgradeable_loader_state_size( loader_state );
  fd_funk_rec_t * acc_data_rec = NULL;

  char *raw_acc_data = fd_acc_mgr_modify_raw(global->acc_mgr, global->funk_txn, (fd_pubkey_t *)  program_acc, 1, encoded_loader_state_size, NULL, &acc_data_rec, &err);
  fd_account_meta_t *m = (fd_account_meta_t *) raw_acc_data;

  fd_bincode_encode_ctx_t ctx;
  ctx.data = raw_acc_data + m->hlen;
  ctx.dataend = (char*)ctx.data + encoded_loader_state_size;

  if ( fd_bpf_upgradeable_loader_state_encode( loader_state, &ctx ) ) {
    FD_LOG_ERR(("fd_bpf_upgradeable_loader_state_encode failed"));
  }

  ulong lamps = (encoded_loader_state_size + 128) * ((ulong) ((double)global->bank.rent.lamports_per_uint8_year * global->bank.rent.exemption_threshold));
  if (m->info.lamports < lamps) {
    FD_LOG_DEBUG(("topped up the lamports.. was this needed?"));
    return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
// m->info.lamports = lamps;
  }

  if (encoded_loader_state_size > m->dlen)
    m->dlen = encoded_loader_state_size;

  return fd_acc_mgr_commit_raw(global->acc_mgr, acc_data_rec, (fd_pubkey_t *) program_acc, raw_acc_data, global->bank.slot, 0);
}

// This is literally called before every single instruction execution... To make it fast we are duplicating some code
int fd_executor_bpf_upgradeable_loader_program_is_executable_program_account( fd_global_ctx_t * global, fd_pubkey_t * pubkey ) {
  int err = 0;
  char * raw_acc_data = (char*) fd_acc_mgr_view_raw(global->acc_mgr, global->funk_txn, (fd_pubkey_t *) pubkey, NULL, &err);
  if (NULL == raw_acc_data)
    return -1;

  fd_account_meta_t *m = (fd_account_meta_t *) raw_acc_data;

  if( memcmp( m->info.owner, global->solana_bpf_loader_upgradeable_program, sizeof(fd_pubkey_t)) )
    return -1;

  if( m->info.executable != 1)
    return -1;

  fd_bincode_decode_ctx_t ctx = {
    .data = raw_acc_data + m->hlen,
    .dataend = (char *) ctx.data + m->dlen,
    .valloc  = global->valloc,
  };

  fd_bpf_upgradeable_loader_state_t loader_state;
  fd_bpf_upgradeable_loader_state_new(&loader_state);
  if ( fd_bpf_upgradeable_loader_state_decode( &loader_state, &ctx ) ) {
    FD_LOG_WARNING(("fd_bpf_upgradeable_loader_state_decode failed"));
    return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
  }

  if( !fd_bpf_upgradeable_loader_state_is_program( &loader_state ) )
    return -1;

  fd_bincode_destroy_ctx_t ctx_d = { .valloc = global->valloc };
  fd_bpf_upgradeable_loader_state_destroy( &loader_state, &ctx_d );

  return 0;
}

/**
 * num accounts
 * serialized accounts
 * instr data len
 * instr data
 * program id public key
*/
// 64-bit aligned
uchar *
serialize_aligned( instruction_ctx_t ctx, ulong * sz ) {
  ulong serialized_size = 0;
  uchar * instr_acc_idxs = ((uchar *)ctx.txn_ctx->txn_raw->raw + ctx.instr->acct_off);
  fd_pubkey_t * txn_accs = (fd_pubkey_t *)((uchar *)ctx.txn_ctx->txn_raw->raw + ctx.txn_ctx->txn_descriptor->acct_addr_off);

  uchar acc_idx_seen[256];
  ushort dup_acc_idx[256];
  memset(acc_idx_seen, 0, sizeof(acc_idx_seen));
  memset(dup_acc_idx, 0, sizeof(dup_acc_idx));

  serialized_size += sizeof(ulong);
  for( ushort i = 0; i < ctx.instr->acct_cnt; i++ ) {
    uchar acc_idx = instr_acc_idxs[i];

    // fd_pubkey_t * acc = &txn_accs[acc_idx];
    // FD_LOG_WARNING(( "START OF ACC: %32J %x %lu", acc, serialized_size, serialized_size ));

    serialized_size++; // dup byte
    if( FD_UNLIKELY( acc_idx_seen[acc_idx] ) ) {
      serialized_size += 7; // pad to 64-bit alignment
    } else {
      acc_idx_seen[acc_idx] = 1;
      dup_acc_idx[acc_idx] = i;
      fd_pubkey_t * acc = &txn_accs[acc_idx];
      int read_result = FD_ACC_MGR_SUCCESS;
      uchar * raw_acc_data = (uchar *)fd_acc_mgr_view_raw(ctx.global->acc_mgr, ctx.global->funk_txn, acc, NULL, &read_result);
      fd_account_meta_t * metadata = (fd_account_meta_t *)raw_acc_data;

      ulong acc_data_len = 0;
      if ( FD_LIKELY( read_result == FD_ACC_MGR_SUCCESS ) ) {
        acc_data_len = metadata->dlen;
      } else if ( FD_UNLIKELY( read_result == FD_ACC_MGR_ERR_UNKNOWN_ACCOUNT ) ) {
        acc_data_len = 0;
      } else {
        FD_LOG_WARNING(( "failed to read account data - pubkey: %32J, err: %d", acc, read_result ));
        return NULL;
      }

      ulong aligned_acc_data_len = fd_ulong_align_up(acc_data_len, 8);

      serialized_size += sizeof(uchar)  // is_signer
          + sizeof(uchar)               // is_writable
          + sizeof(uchar)               // is_executable
          + sizeof(uint)                // original_data_len
          + sizeof(fd_pubkey_t)         // key
          + sizeof(fd_pubkey_t)         // owner
          + sizeof(ulong)               // lamports
          + sizeof(ulong)               // data_len
          + aligned_acc_data_len
          + MAX_PERMITTED_DATA_INCREASE
          + sizeof(ulong);              // rent_epoch
    }
  }

  serialized_size += sizeof(ulong)
      + ctx.instr->data_sz
      + sizeof(fd_pubkey_t);

  uchar * serialized_params = fd_valloc_malloc( ctx.global->valloc, 1UL, serialized_size);
  uchar * serialized_params_start = serialized_params;

  FD_STORE( ulong, serialized_params, ctx.instr->acct_cnt );
  serialized_params += sizeof(ulong);

  for( ushort i = 0; i < ctx.instr->acct_cnt; i++ ) {
    // FD_LOG_WARNING(( "SERIAL OF ACC: %x %lu", serialized_params - serialized_params_start, serialized_params-serialized_params_start ));
    uchar acc_idx = instr_acc_idxs[i];
    fd_pubkey_t * acc = &txn_accs[acc_idx];

    if( FD_UNLIKELY( acc_idx_seen[acc_idx] && dup_acc_idx[acc_idx] != i ) ) {
      // Duplicate
      FD_STORE( ulong, serialized_params, 0 );
      FD_STORE( uchar, serialized_params, (uchar)dup_acc_idx[acc_idx] );
      serialized_params += sizeof(ulong);
    } else {
      FD_STORE( uchar, serialized_params, 0xFF );
      serialized_params += sizeof(uchar);

      int read_result = FD_ACC_MGR_SUCCESS;
      uchar * raw_acc_data = (uchar *)fd_acc_mgr_view_raw(ctx.global->acc_mgr, ctx.global->funk_txn, acc, NULL, &read_result);
      if ( FD_UNLIKELY( read_result == FD_ACC_MGR_ERR_UNKNOWN_ACCOUNT ) ) {
          fd_memset( serialized_params, 0, sizeof(uchar)  // is_signer
          + sizeof(uchar)                                 // is_writable
          + sizeof(uchar)                                 // is_executable
          + sizeof(uint));                                // original_data_len

          serialized_params += sizeof(uchar)  // is_signer
          + sizeof(uchar)                     // is_writable
          + sizeof(uchar)                     // is_executable
          + sizeof(uint);                     // original_data_len

          fd_pubkey_t key = *acc;
          FD_STORE( fd_pubkey_t, serialized_params, key );
          serialized_params += sizeof(fd_pubkey_t);

          fd_memset( serialized_params, 0, sizeof(fd_pubkey_t)  // owner
          + sizeof(ulong)                                       // lamports
          + sizeof(ulong)                                       // data_len
          + 0                                                   // data
          + MAX_PERMITTED_DATA_INCREASE
          + sizeof(ulong));                                     // rent_epoch
          serialized_params += sizeof(fd_pubkey_t)  // owner
          + sizeof(ulong)                           // lamports
          + sizeof(ulong)                           // data_len
          + 0                                       // data
          + MAX_PERMITTED_DATA_INCREASE
          + sizeof(ulong);                          // rent_epoch
        continue;
      } else if ( FD_UNLIKELY( read_result != FD_ACC_MGR_SUCCESS ) ) {
        FD_LOG_WARNING(( "failed to read account data - pubkey: %32J, err: %d", acc, read_result ));
        return NULL;
      }

      fd_account_meta_t * metadata = (fd_account_meta_t *)raw_acc_data;
      uchar * acc_data = fd_account_get_data( metadata );

      uchar is_signer = (uchar)fd_account_is_signer( &ctx, acc );
      FD_STORE( uchar, serialized_params, is_signer );
      serialized_params += sizeof(uchar);

      uchar is_writable = (uchar)(fd_account_is_writable_idx( &ctx, acc_idx ) && !fd_account_is_sysvar( &ctx, acc ));
      FD_STORE( uchar, serialized_params, is_writable );
      serialized_params += sizeof(uchar);

      uchar is_executable = (uchar)metadata->info.executable;
      FD_STORE( uchar, serialized_params, is_executable );
      serialized_params += sizeof(uchar);

      uint padding_0 = 0;
      FD_STORE( uint, serialized_params, padding_0 );
      serialized_params += sizeof(uint);

      fd_pubkey_t key = *acc;
      FD_STORE( fd_pubkey_t, serialized_params, key );
      serialized_params += sizeof(fd_pubkey_t);

      fd_pubkey_t owner = *(fd_pubkey_t *)&metadata->info.owner;
      FD_STORE( fd_pubkey_t, serialized_params, owner );
      serialized_params += sizeof(fd_pubkey_t);

      ulong lamports = metadata->info.lamports;
      FD_STORE( ulong, serialized_params, lamports );
      serialized_params += sizeof(ulong);

      ulong acc_data_len = metadata->dlen;
      ulong aligned_acc_data_len = fd_ulong_align_up(acc_data_len, 8);
      ulong alignment_padding_len = aligned_acc_data_len - acc_data_len;

      ulong data_len = acc_data_len;
      FD_STORE( ulong, serialized_params, data_len );
      serialized_params += sizeof(ulong);

      fd_memcpy( serialized_params, acc_data, acc_data_len);
      serialized_params += acc_data_len;

      fd_memset( serialized_params, 0, MAX_PERMITTED_DATA_INCREASE + alignment_padding_len);
      serialized_params += MAX_PERMITTED_DATA_INCREASE + alignment_padding_len;

      ulong rent_epoch = metadata->info.rent_epoch;
      FD_STORE( ulong, serialized_params, rent_epoch );
      serialized_params += sizeof(ulong);
    }
  }

  ulong instr_data_len = ctx.instr->data_sz;
  FD_STORE( ulong, serialized_params, instr_data_len );
  serialized_params += sizeof(ulong);

  uchar * instr_data = (uchar *)ctx.txn_ctx->txn_raw->raw + ctx.instr->data_off;
  fd_memcpy( serialized_params, instr_data, instr_data_len );
  serialized_params += instr_data_len;

  FD_STORE( fd_pubkey_t, serialized_params, txn_accs[ctx.instr->program_id] );
  serialized_params += sizeof(fd_pubkey_t);

  FD_TEST( serialized_params == serialized_params_start + serialized_size );

  // FD_LOG_NOTICE(( "SERIALIZE - sz: %lu, diff: %lu", serialized_size, serialized_params - serialized_params_start ));
  *sz = serialized_size;
  return serialized_params_start;
}

int
deserialize_aligned( instruction_ctx_t ctx, uchar * input, ulong input_sz ) {
  uchar * input_cursor = input;

  uchar acc_idx_seen[256];
  memset(acc_idx_seen, 0, sizeof(acc_idx_seen));

  uchar * instr_acc_idxs = ((uchar *)ctx.txn_ctx->txn_raw->raw + ctx.instr->acct_off);
  fd_pubkey_t * txn_accs = (fd_pubkey_t *)((uchar *)ctx.txn_ctx->txn_raw->raw + ctx.txn_ctx->txn_descriptor->acct_addr_off);

  input_cursor += sizeof(ulong);

  for( ulong i = 0; i < ctx.instr->acct_cnt; i++ ) {
    uchar acc_idx = instr_acc_idxs[i];
    fd_pubkey_t * acc = &txn_accs[instr_acc_idxs[i]];

    input_cursor++;
    if ( FD_UNLIKELY( acc_idx_seen[acc_idx] ) ) {
      input_cursor += 7;
    } else if ( fd_account_is_writable_idx( &ctx, acc_idx ) && !fd_account_is_sysvar( &ctx, acc ) ) {
      acc_idx_seen[acc_idx] = 1;
      input_cursor += sizeof(uchar) // is_signer
          + sizeof(uchar)           // is_writable
          + sizeof(uchar)           // executable
          + sizeof(uint)            // original_data_len
          + sizeof(fd_pubkey_t);    // key

      fd_pubkey_t * owner = (fd_pubkey_t *)input_cursor;
      input_cursor += sizeof(fd_pubkey_t);

      ulong lamports = FD_LOAD(ulong, input_cursor);
      input_cursor += sizeof(ulong);

      ulong post_data_len = FD_LOAD(ulong, input_cursor);
      input_cursor += sizeof(ulong);

      uchar * post_data = input_cursor;

      fd_funk_rec_t const * acc_const_data_rec = NULL;
      int view_err = FD_ACC_MGR_SUCCESS;
      void const * raw_data = fd_acc_mgr_view_raw(ctx.global->acc_mgr, ctx.global->funk_txn, acc, &acc_const_data_rec, &view_err);

      if ( view_err == FD_ACC_MGR_SUCCESS ) {
        fd_account_meta_t * metadata = (fd_account_meta_t *)raw_data;
        if ( fd_ulong_sat_sub( post_data_len, metadata->dlen ) > MAX_PERMITTED_DATA_INCREASE || post_data_len > MAX_PERMITTED_DATA_LENGTH ) {
          fd_valloc_free( ctx.global->valloc, input );
          return -1;
        }

        fd_funk_rec_t * acc_data_rec = NULL;
        int modify_err = FD_ACC_MGR_SUCCESS;
        void * raw_acc_data = fd_acc_mgr_modify_raw(ctx.global->acc_mgr, ctx.global->funk_txn, acc, 0, post_data_len, acc_const_data_rec, &acc_data_rec, &modify_err);
        if ( modify_err != FD_ACC_MGR_SUCCESS ) {
          fd_valloc_free( ctx.global->valloc, input );
          return -1;
        }
        metadata = (fd_account_meta_t *)raw_acc_data;

        uchar * acc_data = fd_account_get_data( metadata );
        input_cursor += fd_ulong_align_up(metadata->dlen, 8);

        metadata->dlen = post_data_len;
        metadata->info.lamports = lamports;
        fd_memcpy(metadata->info.owner, owner, sizeof(fd_pubkey_t));

        fd_memcpy( acc_data, post_data, post_data_len );

        fd_acc_mgr_commit_raw(ctx.global->acc_mgr, acc_data_rec, acc, raw_acc_data, ctx.global->bank.slot, 0);
      } else if ( view_err == FD_ACC_MGR_ERR_UNKNOWN_ACCOUNT ) {
        // no-op
      } else {
        fd_valloc_free( ctx.global->valloc, input );
        return -1;
      }

      input_cursor += MAX_PERMITTED_DATA_INCREASE;

      input_cursor += sizeof(ulong);
    } else {
      acc_idx_seen[acc_idx] = 1;
      // Account is not writable, skip over
      input_cursor += sizeof(uchar)         // is_signer
          + sizeof(uchar)                   // is_writable
          + sizeof(uchar)                   // executable
          + sizeof(uint)                    // original_data_len
          + sizeof(fd_pubkey_t);            // key
      input_cursor += sizeof(fd_pubkey_t);  // owner
      input_cursor += sizeof(ulong);        // lamports
      input_cursor += sizeof(ulong);        // data_len

      int view_err = FD_ACC_MGR_SUCCESS;
      void const * raw_acc_data = fd_acc_mgr_view_raw(ctx.global->acc_mgr, ctx.global->funk_txn, (fd_pubkey_t const *)acc, NULL, &view_err);
      fd_account_meta_t * metadata = (fd_account_meta_t *)raw_acc_data;

      if ( view_err == FD_ACC_MGR_SUCCESS ) {
        input_cursor += fd_ulong_align_up(metadata->dlen, 8);
      }
      input_cursor += MAX_PERMITTED_DATA_INCREASE;

      input_cursor += sizeof(ulong);
    }
  }

  FD_TEST( input_cursor <= input + input_sz );

  fd_valloc_free( ctx.global->valloc, input );

  return 0;
}

int fd_executor_bpf_upgradeable_loader_program_execute_program_instruction( instruction_ctx_t ctx ) {
  fd_pubkey_t * txn_accs = (fd_pubkey_t *)((uchar *)ctx.txn_ctx->txn_raw->raw + ctx.txn_ctx->txn_descriptor->acct_addr_off);
  fd_pubkey_t * program_acc = &txn_accs[ctx.instr->program_id];

  fd_bpf_upgradeable_loader_state_t program_loader_state;
  int err = 0;
  if (FD_UNLIKELY(NULL == read_bpf_upgradeable_loader_state( ctx.global, program_acc, &program_loader_state, &err )))
    return err;

  fd_bincode_destroy_ctx_t ctx_d = { .valloc = ctx.global->valloc };

  if( !fd_bpf_upgradeable_loader_state_is_program( &program_loader_state ) ) {
    fd_bpf_upgradeable_loader_state_destroy( &program_loader_state, &ctx_d );
    return -1;
  }

  fd_pubkey_t * programdata_acc = &program_loader_state.inner.program.programdata_address;

  fd_bpf_upgradeable_loader_state_t programdata_loader_state;

  err = 0;
  uchar *ptr = (uchar *) read_bpf_upgradeable_loader_state( ctx.global, programdata_acc, &programdata_loader_state, &err );
  if (NULL == ptr)
    return err;
  fd_account_meta_t *programdata_metadata = (fd_account_meta_t *) ptr;

  FD_LOG_NOTICE(("BPF PROG INSTR RUN! - slot: %lu, addr: %32J", ctx.global->bank.slot, &txn_accs[ctx.instr->program_id]));

  if( !fd_bpf_upgradeable_loader_state_is_program_data( &programdata_loader_state ) ) {
    fd_bpf_upgradeable_loader_state_destroy( &programdata_loader_state, &ctx_d );
    fd_bpf_upgradeable_loader_state_destroy( &program_loader_state, &ctx_d );
    return -1;
  }
  fd_bpf_upgradeable_loader_state_destroy( &programdata_loader_state, &ctx_d );

  ulong program_data_len = programdata_metadata->dlen - PROGRAMDATA_METADATA_SIZE;
  uchar * program_data = ptr + programdata_metadata->hlen + PROGRAMDATA_METADATA_SIZE;

  fd_bpf_upgradeable_loader_state_destroy( &program_loader_state, &ctx_d );

  fd_sbpf_elf_info_t elf_info;
  fd_sbpf_elf_peek( &elf_info, program_data, program_data_len );

  /* Allocate rodata segment */

  void * rodata = fd_valloc_malloc( ctx.global->valloc, 1UL,  elf_info.rodata_footprint );
  FD_TEST( rodata );

  /* Allocate program buffer */

  ulong  prog_align     = fd_sbpf_program_align();
  ulong  prog_footprint = fd_sbpf_program_footprint( &elf_info );
  fd_sbpf_program_t * prog = fd_sbpf_program_new( fd_valloc_malloc( ctx.global->valloc, prog_align, prog_footprint ), &elf_info, rodata );
  FD_TEST( prog );

  /* Allocate syscalls */

  fd_sbpf_syscalls_t * syscalls = fd_sbpf_syscalls_new( fd_valloc_malloc( ctx.global->valloc, fd_sbpf_syscalls_align(), fd_sbpf_syscalls_footprint() ) );
  FD_TEST( syscalls );

  fd_vm_syscall_register_all( syscalls );
  /* Load program */

  if(  0!=fd_sbpf_program_load( prog, program_data, program_data_len, syscalls ) ) {
    FD_LOG_ERR(( "fd_sbpf_program_load() failed: %s", fd_sbpf_strerror() ));
  }
  FD_LOG_DEBUG(( "fd_sbpf_program_load() success: %s", fd_sbpf_strerror() ));

  ulong input_sz = 0;
  uchar * input = serialize_aligned(ctx, &input_sz);
  if( input==NULL ) {
    fd_valloc_free( ctx.global->valloc, fd_sbpf_program_delete( prog ) );
    fd_valloc_free( ctx.global->valloc, fd_sbpf_syscalls_delete( syscalls ) );
    fd_valloc_free( ctx.global->valloc, rodata);
    return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
  }
  fd_vm_exec_context_t vm_ctx = {
    .entrypoint          = (long)prog->entry_pc,
    .program_counter     = 0,
    .instruction_counter = 0,
    .instrs              = (fd_sbpf_instr_t const *)fd_type_pun_const( prog->text ),
    .instrs_sz           = prog->text_cnt,
    .instrs_offset       = prog->text_off,
    .syscall_map         = syscalls,
    .local_call_map      = prog->calldests,
    .input               = input,
    .input_sz            = input_sz,
    .read_only           = (uchar *)fd_type_pun_const(prog->rodata),
    .read_only_sz        = prog->rodata_sz,
    /* TODO configure heap allocator */
    .instr_ctx           = ctx,
  };

  ulong trace_sz = 16 * 1024 * 1024;
  ulong trace_used = 0;
  // fd_vm_trace_entry_t * trace = (fd_vm_trace_entry_t *)fd_valloc_malloc( ctx.global->valloc, 1UL, trace_sz * sizeof(fd_vm_trace_entry_t));
  fd_vm_trace_entry_t * trace = (fd_vm_trace_entry_t *)malloc( trace_sz * sizeof(fd_vm_trace_entry_t));

  memset(vm_ctx.register_file, 0, sizeof(vm_ctx.register_file));
  vm_ctx.register_file[1] = FD_VM_MEM_MAP_INPUT_REGION_START;
  vm_ctx.register_file[10] = FD_VM_MEM_MAP_STACK_REGION_START + 0x1000;


  // ulong validate_result = fd_vm_context_validate( &vm_ctx );
  // if (validate_result != FD_VM_SBPF_VALIDATE_SUCCESS) {
  //   FD_LOG_ERR(( "fd_vm_context_validate() failed: %lu", validate_result ));
  // }

  // FD_LOG_WARNING(( "fd_vm_context_validate() success" ));

  ulong interp_res = fd_vm_interp_instrs_trace( &vm_ctx, trace, trace_sz, &trace_used );
  if( interp_res != 0 ) {
    FD_LOG_ERR(( "fd_vm_interp_instrs() failed: %lu", interp_res ));
  }

  // TODO: make tracing an option!
  // FILE * trace_fd = fopen("trace.log", "w");

  // for( ulong i = 0; i < trace_used; i++ ) {
  //   fd_vm_trace_entry_t trace_ent = trace[i];
  //   fprintf(stderr, "%5lu [%016lX, %016lX, %016lX, %016lX, %016lX, %016lX, %016lX, %016lX, %016lX, %016lX, %016lX] %5lu: ",
  //       trace_ent.ic,
  //       trace_ent.register_file[0],
  //       trace_ent.register_file[1],
  //       trace_ent.register_file[2],
  //       trace_ent.register_file[3],
  //       trace_ent.register_file[4],
  //       trace_ent.register_file[5],
  //       trace_ent.register_file[6],
  //       trace_ent.register_file[7],
  //       trace_ent.register_file[8],
  //       trace_ent.register_file[9],
  //       trace_ent.register_file[10],
  //       trace_ent.pc+29 // FIXME: THIS OFFSET IS FOR TESTING ONLY
  //     );
  //   fd_vm_disassemble_instr(&vm_ctx.instrs[trace[i].pc], trace[i].pc, vm_ctx.syscall_map, vm_ctx.local_call_map, stderr);

  //   fprintf(stderr, "\n");
  // }

  // fclose(trace_fd);
  free(trace);
  // fd_valloc_free( ctx.global->valloc, trace);

  fd_valloc_free( ctx.global->valloc, fd_sbpf_program_delete( prog ) );
  fd_valloc_free( ctx.global->valloc, fd_sbpf_syscalls_delete( syscalls ) );
  fd_valloc_free( ctx.global->valloc, rodata);

  FD_LOG_WARNING(( "fd_vm_interp_instrs() success: %lu, ic: %lu, pc: %lu, ep: %lu, r0: %lu, fault: %lu", interp_res, vm_ctx.instruction_counter, vm_ctx.program_counter, vm_ctx.entrypoint, vm_ctx.register_file[0], vm_ctx.cond_fault ));
  FD_LOG_WARNING(( "log coll: %s", vm_ctx.log_collector.buf ));

  if( vm_ctx.register_file[0]!=0 ) {
    fd_valloc_free( ctx.global->valloc, input);
    // TODO: vm should report this error
    return -1;
  }

  if( vm_ctx.cond_fault ) {
    fd_valloc_free( ctx.global->valloc, input);
    // TODO: vm should report this error
    return -1;
  }

  if( deserialize_aligned(ctx, input, input_sz) != 0 ) {
    return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
  }

  return 0;
}

static int setup_program(instruction_ctx_t ctx, const uchar * program_data, ulong program_data_len) {
  fd_sbpf_elf_info_t elf_info;
  fd_sbpf_elf_peek( &elf_info, program_data, program_data_len );

  /* Allocate rodata segment */
  void * rodata = fd_valloc_malloc( ctx.global->valloc, 1UL,  elf_info.rodata_footprint );
  if (!rodata) {
    return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
  }

  /* Allocate program buffer */

  ulong  prog_align     = fd_sbpf_program_align();
  ulong  prog_footprint = fd_sbpf_program_footprint( &elf_info );
  fd_sbpf_program_t * prog = fd_sbpf_program_new( fd_valloc_malloc( ctx.global->valloc, prog_align, prog_footprint ), &elf_info, rodata );
  FD_TEST( prog );

  /* Allocate syscalls */

  fd_sbpf_syscalls_t * syscalls = fd_sbpf_syscalls_new( fd_valloc_malloc( ctx.global->valloc, fd_sbpf_syscalls_align(), fd_sbpf_syscalls_footprint() ) );
  FD_TEST( syscalls );

  fd_vm_syscall_register_all( syscalls );
  /* Load program */

  if(  0!=fd_sbpf_program_load( prog, program_data, program_data_len, syscalls ) ) {
    FD_LOG_ERR(( "fd_sbpf_program_load() failed: %s", fd_sbpf_strerror() ));
  }
  FD_LOG_WARNING(( "fd_sbpf_program_load() success: %s", fd_sbpf_strerror() ));

  fd_vm_exec_context_t vm_ctx = {
    .entrypoint          = (long)prog->entry_pc,
    .program_counter     = 0,
    .instruction_counter = 0,
    .instrs              = (fd_sbpf_instr_t const *)fd_type_pun_const( prog->text ),
    .instrs_sz           = prog->text_cnt,
    .instrs_offset       = prog->text_off,
    .syscall_map         = syscalls,
    .local_call_map      = prog->calldests,
    .input               = NULL,
    .input_sz            = 0,
    .read_only           = (uchar *)fd_type_pun_const(prog->rodata),
    .read_only_sz        = prog->rodata_sz,
    /* TODO configure heap allocator */
    .instr_ctx           = ctx,
  };

  ulong validate_result = fd_vm_context_validate( &vm_ctx );
  if (validate_result != FD_VM_SBPF_VALIDATE_SUCCESS) {
    FD_LOG_ERR(( "fd_vm_context_validate() failed: %lu", validate_result ));
  }

  FD_LOG_WARNING(( "fd_vm_context_validate() success" ));

  fd_valloc_free( ctx.global->valloc,  fd_sbpf_program_delete( prog ) );
  fd_valloc_free( ctx.global->valloc,  fd_sbpf_syscalls_delete( syscalls ) );
  fd_valloc_free( ctx.global->valloc, rodata);
  return 0;
}

static int set_executable(instruction_ctx_t ctx, fd_pubkey_t * program_acc, fd_account_meta_t * metadata, char is_executable) {
  fd_rent_t rent;
  fd_rent_new( &rent );
  if (fd_sysvar_rent_read( ctx.global, &rent ) == 0) {
    ulong min_balance = fd_rent_exempt_minimum_balance(ctx.global, metadata->dlen);
    if (metadata->info.lamports < min_balance) {
      return FD_EXECUTOR_INSTR_ERR_EXECUTABLE_ACCOUNT_NOT_RENT_EXEMPT;
    }

    if (0 != memcmp(metadata->info.owner, ctx.global->solana_bpf_loader_program, sizeof(fd_pubkey_t))) {
      return FD_EXECUTOR_INSTR_ERR_EXECUTABLE_MODIFIED;
    }

    if (!fd_account_is_writable(&ctx, program_acc)) {
      return FD_EXECUTOR_INSTR_ERR_EXECUTABLE_MODIFIED;
    }

    if (metadata->info.executable && !is_executable) {
      return FD_EXECUTOR_INSTR_ERR_EXECUTABLE_MODIFIED;
    }

    if (metadata->info.executable == is_executable) {
      return 0;
    }
  }

  metadata->info.executable = is_executable;
  return 0;
}

int fd_executor_bpf_upgradeable_loader_program_execute_instruction( instruction_ctx_t ctx ) {
  /* Deserialize the Stake instruction */
  uchar * data            = (uchar *)ctx.txn_ctx->txn_raw->raw + ctx.instr->data_off;

  fd_bpf_upgradeable_loader_program_instruction_t instruction;
  fd_bpf_upgradeable_loader_program_instruction_new( &instruction );
  fd_bincode_decode_ctx_t decode_ctx;
  decode_ctx.data = data;
  decode_ctx.dataend = &data[ctx.instr->data_sz];
  decode_ctx.valloc  = ctx.global->valloc;

  int decode_err;
  if ( ( decode_err = fd_bpf_upgradeable_loader_program_instruction_decode( &instruction, &decode_ctx ) ) ) {
    FD_LOG_DEBUG(("fd_bpf_upgradeable_loader_program_instruction_decode failed: err code: %d, %ld", decode_err, ctx.instr->data_sz));
    return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
  }


  uchar* instr_acc_idxs = ((uchar *)ctx.txn_ctx->txn_raw->raw + ctx.instr->acct_off);
  fd_pubkey_t* txn_accs = (fd_pubkey_t *)((uchar *)ctx.txn_ctx->txn_raw->raw + ctx.txn_ctx->txn_descriptor->acct_addr_off);


  FD_LOG_INFO(("BPF INSTR RUN! - addr: %32J, disc: %u", &txn_accs[ctx.instr->program_id], instruction.discriminant));

  if( fd_bpf_upgradeable_loader_program_instruction_is_initialize_buffer( &instruction ) ) {
    if( ctx.instr->acct_cnt < 2 ) {
      return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
    }

    fd_bpf_upgradeable_loader_state_t loader_state;
    fd_pubkey_t * buffer_acc = &txn_accs[instr_acc_idxs[0]];

    int err = 0;
    if (FD_UNLIKELY(NULL == read_bpf_upgradeable_loader_state( ctx.global, buffer_acc, &loader_state, &err ))) {
      // TODO: Fix leaks...
      return err;
    }

    if( !fd_bpf_upgradeable_loader_state_is_uninitialized( &loader_state ) ) {
      return FD_EXECUTOR_INSTR_ERR_ACC_ALREADY_INITIALIZED;
    }

    fd_pubkey_t * authority_acc = &txn_accs[instr_acc_idxs[1]];
    loader_state.discriminant = fd_bpf_upgradeable_loader_state_enum_buffer;
    loader_state.inner.buffer.authority_address = authority_acc;

    return write_bpf_upgradeable_loader_state( ctx.global, buffer_acc, &loader_state );
  } else if ( fd_bpf_upgradeable_loader_program_instruction_is_write( &instruction ) ) {
    if( ctx.instr->acct_cnt < 2 ) {
      return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
    }

    // FIXME: Do we need to check writable?

    fd_pubkey_t * buffer_acc = &txn_accs[instr_acc_idxs[0]];
    fd_pubkey_t * authority_acc = &txn_accs[instr_acc_idxs[1]];

    fd_bpf_upgradeable_loader_state_t loader_state;
    int err = 0;
    if (NULL == read_bpf_upgradeable_loader_state( ctx.global, buffer_acc, &loader_state, &err)) {
      return err;
    }

    if( !fd_bpf_upgradeable_loader_state_is_buffer( &loader_state ) ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
    }

    if( loader_state.inner.buffer.authority_address==NULL ) {
      return FD_EXECUTOR_INSTR_ERR_ACC_IMMUTABLE;
    }

    if( memcmp( authority_acc, loader_state.inner.buffer.authority_address, sizeof(fd_pubkey_t) )!=0 ) {
      return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
    }

    if(instr_acc_idxs[1] >= ctx.txn_ctx->txn_descriptor->signature_cnt) {
      return FD_EXECUTOR_INSTR_ERR_MISSING_REQUIRED_SIGNATURE;
    }

    fd_funk_rec_t const * buffer_con_rec = NULL;
    int read_result = 0;
    uchar const * buffer_raw = fd_acc_mgr_view_raw( ctx.global->acc_mgr, ctx.global->funk_txn, buffer_acc, &buffer_con_rec, &read_result );
    if( FD_UNLIKELY( !buffer_raw ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
    }
    fd_account_meta_t const * buffer_acc_metadata = (fd_account_meta_t const *)buffer_raw;

    ulong offset = fd_ulong_sat_add(fd_bpf_upgradeable_loader_state_size( &loader_state ), instruction.inner.write.offset);
    ulong write_end = fd_ulong_sat_add( offset, instruction.inner.write.bytes_len );
    if( buffer_acc_metadata->dlen < write_end ) {
      return FD_EXECUTOR_INSTR_ERR_ACC_DATA_TOO_SMALL;
    }

    int write_result = 0;
    uchar * raw_mut = fd_acc_mgr_modify_raw( ctx.global->acc_mgr, ctx.global->funk_txn, buffer_acc, 0, 0UL, buffer_con_rec, NULL, &write_result );
    if( FD_UNLIKELY( !raw_mut ) ) {
      FD_LOG_WARNING(( "failed to get writable handle to buffer data" ));
      return write_result;
    }

    fd_account_meta_t * metadata_mut    = (fd_account_meta_t *)raw_mut;
    uchar *             buffer_acc_data = raw_mut + metadata_mut->hlen;

    fd_memcpy( buffer_acc_data + offset, instruction.inner.write.bytes, instruction.inner.write.bytes_len );
    return FD_EXECUTOR_INSTR_SUCCESS;

  } else if ( fd_bpf_upgradeable_loader_program_instruction_is_deploy_with_max_data_len( &instruction ) ) {
    if( ctx.instr->acct_cnt < 4 ) {
      return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
    }

    fd_pubkey_t * payer_acc       = &txn_accs[instr_acc_idxs[0]];
    fd_pubkey_t * programdata_acc = &txn_accs[instr_acc_idxs[1]];
    fd_pubkey_t * program_acc     = &txn_accs[instr_acc_idxs[2]];
    fd_pubkey_t * buffer_acc      = &txn_accs[instr_acc_idxs[3]];
    fd_pubkey_t * rent_acc        = &txn_accs[instr_acc_idxs[4]];
    fd_pubkey_t * clock_acc       = &txn_accs[instr_acc_idxs[5]];

    if( ctx.instr->acct_cnt < 8 ) {
      return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
    }
    fd_pubkey_t * authority_acc = &txn_accs[instr_acc_idxs[7]];

    fd_account_meta_t const * program_acc_metadata = NULL;
    int result = fd_acc_mgr_view(ctx.global->acc_mgr, ctx.global->funk_txn, program_acc, NULL, &program_acc_metadata, NULL);
    if( FD_UNLIKELY( result != FD_ACC_MGR_SUCCESS ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
    }

    fd_bpf_upgradeable_loader_state_t program_loader_state;

    int err = 0;
    uchar *ptr = (uchar *) read_bpf_upgradeable_loader_state( ctx.global, program_acc, &program_loader_state, &err );
    if (NULL == ptr)
      return err;

    if (!fd_bpf_upgradeable_loader_state_is_uninitialized(&program_loader_state)) {
      return FD_EXECUTOR_INSTR_ERR_ACC_ALREADY_INITIALIZED;
    }

    if (program_acc_metadata->dlen < SIZE_OF_PROGRAM) {
      return FD_EXECUTOR_INSTR_ERR_ACC_DATA_TOO_SMALL;
    }

    if (program_acc_metadata->info.lamports < fd_rent_exempt_minimum_balance(ctx.global, program_acc_metadata->dlen)) {
      return FD_EXECUTOR_INSTR_ERR_EXECUTABLE_ACCOUNT_NOT_RENT_EXEMPT;
    }

    fd_account_meta_t const * buffer_acc_metadata = NULL;
    result = fd_acc_mgr_view(ctx.global->acc_mgr, ctx.global->funk_txn, buffer_acc, NULL, &buffer_acc_metadata, NULL);
    if( FD_UNLIKELY( result != FD_ACC_MGR_SUCCESS ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
    }

    fd_bpf_upgradeable_loader_state_t buffer_acc_loader_state;
    err = 0;
    if (NULL == read_bpf_upgradeable_loader_state( ctx.global, buffer_acc, &buffer_acc_loader_state, &err )) {
      FD_LOG_DEBUG(( "failed to read account metadata" ));
      return err;
    }
    if( !fd_bpf_upgradeable_loader_state_is_buffer( &buffer_acc_loader_state ) ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    if( buffer_acc_loader_state.inner.buffer.authority_address==NULL ) {
      return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
    }

    if( memcmp( buffer_acc_loader_state.inner.buffer.authority_address, authority_acc, sizeof(fd_pubkey_t) ) != 0 ) {
      return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
    }

    if( instr_acc_idxs[7] >= ctx.txn_ctx->txn_descriptor->signature_cnt ) {
      return FD_EXECUTOR_INSTR_ERR_MISSING_REQUIRED_SIGNATURE;
    }

    ulong buffer_data_len = fd_ulong_sat_sub(buffer_acc_metadata->dlen, BUFFER_METADATA_SIZE);
    ulong programdata_len = fd_ulong_sat_add(PROGRAMDATA_METADATA_SIZE, instruction.inner.deploy_with_max_data_len.max_data_len);
    if (buffer_acc_metadata->dlen < BUFFER_METADATA_SIZE || buffer_data_len == 0) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
    }

    if (instruction.inner.deploy_with_max_data_len.max_data_len < buffer_data_len) {
      return FD_EXECUTOR_INSTR_ERR_ACC_DATA_TOO_SMALL;
    }

    if (programdata_len > MAX_PERMITTED_DATA_LENGTH) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    // let (derived_address, bump_seed) =
    //             Pubkey::find_program_address(&[new_program_id.as_ref()], program_id);
    //         if derived_address != programdata_key {
    //             ic_logger_msg!(log_collector, "ProgramData address is not derived");
    //             return Err(InstructionError::InvalidArgument);
    //         }

    fd_account_meta_t * payer_acc_metadata = NULL;
    int write_result;
    uchar *             buffer_acc_data     = NULL;
    // Drain buffer lamports to payer
    {
      fd_account_meta_t * buffer_acc_metadata = NULL;
      write_result = fd_acc_mgr_modify( ctx.global->acc_mgr, ctx.global->funk_txn, buffer_acc, 0, 0UL, NULL, NULL, &buffer_acc_metadata, &buffer_acc_data );
      if( FD_UNLIKELY( write_result != FD_ACC_MGR_SUCCESS ) ) {
        FD_LOG_WARNING(( "failed to read account metadata" ));
        return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
      }

      write_result = fd_acc_mgr_modify( ctx.global->acc_mgr, ctx.global->funk_txn, payer_acc, 0, 0UL, NULL, NULL, &payer_acc_metadata, NULL );
      if ( FD_UNLIKELY( write_result != FD_ACC_MGR_SUCCESS ) ) {
        FD_LOG_WARNING(( "failed to read account metadata" ));
        return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
      }

      // FIXME: Do checked addition
      payer_acc_metadata ->info.lamports += buffer_acc_metadata->info.lamports;
      buffer_acc_metadata->info.lamports  = 0;
    }
    
    // TODO: deploy program
    err = setup_program(ctx, buffer_acc_data, SIZE_OF_PROGRAM + programdata_len);
    if (err != 0) {
      return err;
    }
    
    // Create program data account
    fd_funk_rec_t * program_data_rec = NULL;
    int modify_err;
    ulong sz2 = PROGRAMDATA_METADATA_SIZE + instruction.inner.deploy_with_max_data_len.max_data_len;
    void * program_data_raw = fd_acc_mgr_modify_raw(ctx.global->acc_mgr, ctx.global->funk_txn, programdata_acc, 1, sz2, NULL, &program_data_rec, &modify_err);
    fd_account_meta_t * meta = (fd_account_meta_t *)program_data_raw;
    uchar * acct_data = fd_account_get_data(meta);

    fd_bpf_upgradeable_loader_state_t program_data_acc_loader_state = {
      .discriminant = fd_bpf_upgradeable_loader_state_enum_program_data,
      .inner.program_data.slot = ctx.global->bank.slot,
      .inner.program_data.upgrade_authority_address = authority_acc
    };

    fd_bincode_encode_ctx_t encode_ctx;
    encode_ctx.data = acct_data;
    encode_ctx.dataend = acct_data + fd_bpf_upgradeable_loader_state_size(&program_data_acc_loader_state);
    if ( fd_bpf_upgradeable_loader_state_encode( &program_data_acc_loader_state, &encode_ctx ) ) {
      FD_LOG_ERR(("fd_bpf_upgradeable_loader_state_encode failed"));
      fd_memset( acct_data, 0, fd_bpf_upgradeable_loader_state_size(&program_data_acc_loader_state) );
      return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
    }

    meta->dlen = PROGRAMDATA_METADATA_SIZE + instruction.inner.deploy_with_max_data_len.max_data_len;
    meta->info.executable = 0;
    fd_memcpy(&meta->info.owner, &ctx.global->solana_bpf_loader_upgradeable_program, sizeof(fd_pubkey_t));
    meta->info.lamports = fd_rent_exempt_minimum_balance(ctx.global, meta->dlen);
    meta->info.rent_epoch = 0;

    payer_acc_metadata->info.lamports += buffer_acc_metadata->info.lamports;
    payer_acc_metadata->info.lamports -= meta->info.lamports;
    buffer_data_len = fd_ulong_sat_sub(buffer_acc_metadata->dlen, BUFFER_METADATA_SIZE);

    uchar * raw_acc_data = (uchar *)fd_acc_mgr_view_raw(ctx.global->acc_mgr, ctx.global->funk_txn, buffer_acc, NULL, &write_result);
    fd_memcpy( acct_data+PROGRAMDATA_METADATA_SIZE, raw_acc_data+BUFFER_METADATA_SIZE+sizeof(fd_account_meta_t), buffer_data_len );
    // fd_memset( acct_data+PROGRAMDATA_METADATA_SIZE+buffer_data_len, 0, instruction.inner.deploy_with_max_data_len.max_data_len-buffer_data_len );
      // FD_LOG_WARNING(("AAA: %x", *(acct_data+meta->dlen-3)));
    fd_acc_mgr_commit_raw(ctx.global->acc_mgr, program_data_rec, programdata_acc, program_data_raw, ctx.global->bank.slot, 0);

    fd_account_meta_t * program_acc_metadata_new = NULL;
    write_result = fd_acc_mgr_modify( ctx.global->acc_mgr, ctx.global->funk_txn, program_acc, 0, 0UL, NULL, NULL, &program_acc_metadata_new, NULL );
    if ( FD_UNLIKELY( write_result != FD_ACC_MGR_SUCCESS ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
    }

    program_acc_metadata_new->info.executable = 1;

    fd_bpf_upgradeable_loader_state_t program_acc_loader_state;
    // FIXME: HANDLE ERRORS!
    err = 0;
    if (NULL == read_bpf_upgradeable_loader_state( ctx.global, program_acc, &program_acc_loader_state, &err ))
      return err;

    program_acc_loader_state.discriminant = fd_bpf_upgradeable_loader_state_enum_program;
    fd_memcpy(&program_acc_loader_state.inner.program.programdata_address, programdata_acc, sizeof(fd_pubkey_t));

    write_result = write_bpf_upgradeable_loader_state( ctx.global, program_acc, &program_acc_loader_state );
    if( FD_UNLIKELY( write_result != FD_ACC_MGR_SUCCESS ) ) {
      FD_LOG_DEBUG(( "failed to write loader state "));
      return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
    }

    err = set_executable(ctx, program_acc, program_acc_metadata_new, 1);
    if (err != 0)
      return err;

    (void)clock_acc;
    (void)rent_acc;

    return FD_EXECUTOR_INSTR_SUCCESS;
  } else if ( fd_bpf_upgradeable_loader_program_instruction_is_upgrade( &instruction ) ) {
    if( ctx.instr->acct_cnt < 7 ) {
      return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
    }

    fd_pubkey_t * programdata_acc = &txn_accs[instr_acc_idxs[0]];
    fd_pubkey_t * program_acc = &txn_accs[instr_acc_idxs[1]];
    fd_pubkey_t * buffer_acc = &txn_accs[instr_acc_idxs[2]];
    fd_pubkey_t * rent_acc = &txn_accs[instr_acc_idxs[4]];
    fd_pubkey_t * clock_acc = &txn_accs[instr_acc_idxs[5]];
    fd_pubkey_t * authority_acc = &txn_accs[instr_acc_idxs[6]];

    fd_account_meta_t const * program_acc_metadata = NULL;
    int read_result = fd_acc_mgr_view( ctx.global->acc_mgr, ctx.global->funk_txn, program_acc, NULL, &program_acc_metadata, NULL );
    if( FD_UNLIKELY( read_result != FD_ACC_MGR_SUCCESS ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
    }

    fd_sol_sysvar_clock_t clock;
    fd_sysvar_clock_read( ctx.global, &clock );

    // Is program executable?
    if( !program_acc_metadata->info.executable ) {
      return FD_EXECUTOR_INSTR_ERR_ACC_NOT_EXECUTABLE;
    }

    // Is program writable?
    if( !fd_txn_is_writable( ctx.txn_ctx->txn_descriptor, instr_acc_idxs[1] ) ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    // Is program owner the BPF upgradeable loader?
    if ( memcmp( program_acc_metadata->info.owner, ctx.global->solana_bpf_loader_upgradeable_program, sizeof(fd_pubkey_t) ) != 0 ) {
      return FD_EXECUTOR_INSTR_ERR_INCORRECT_PROGRAM_ID;
    }

    fd_bpf_upgradeable_loader_state_t program_acc_loader_state;
    int err = 0;
    if (NULL == read_bpf_upgradeable_loader_state( ctx.global, program_acc, &program_acc_loader_state, &err)) {
      FD_LOG_DEBUG(( "failed to read account metadata" ));
      return err;
    }

    if( !fd_bpf_upgradeable_loader_state_is_program( &program_acc_loader_state ) ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
    }

    if( memcmp( &program_acc_loader_state.inner.program.programdata_address, programdata_acc, sizeof(fd_pubkey_t) ) != 0 ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    fd_bpf_upgradeable_loader_state_t buffer_acc_loader_state;
    err = 0;
    if (NULL == read_bpf_upgradeable_loader_state( ctx.global, buffer_acc, &buffer_acc_loader_state, &err )) {
      FD_LOG_DEBUG(( "failed to read account metadata" ));
      return err;
    }
    if( !fd_bpf_upgradeable_loader_state_is_buffer( &buffer_acc_loader_state ) ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    if( buffer_acc_loader_state.inner.buffer.authority_address==NULL ) {
      return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
    }

    if( memcmp( buffer_acc_loader_state.inner.buffer.authority_address, authority_acc, sizeof(fd_pubkey_t) ) != 0 ) {
      return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
    }

    if( instr_acc_idxs[6] >= ctx.txn_ctx->txn_descriptor->signature_cnt ) {
      return FD_EXECUTOR_INSTR_ERR_MISSING_REQUIRED_SIGNATURE;
    }

    uchar const * buffer_raw = fd_acc_mgr_view_raw( ctx.global->acc_mgr, ctx.global->funk_txn, buffer_acc, NULL, &read_result );
    if( FD_UNLIKELY( !buffer_raw ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
    }
    fd_account_meta_t const * buffer_acc_metadata = (fd_account_meta_t const *)buffer_raw;
    uchar const *             buffer_acc_data     = buffer_raw + buffer_acc_metadata->hlen;

    ulong buffer_data_len = fd_ulong_sat_sub(buffer_acc_metadata->dlen, BUFFER_METADATA_SIZE);
    ulong buffer_lamports = buffer_acc_metadata->info.lamports;
    if( buffer_acc_metadata->dlen < BUFFER_METADATA_SIZE || buffer_data_len==0 ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
    }

    ulong sz2 = PROGRAMDATA_METADATA_SIZE + buffer_data_len;
    err = 0;
    void * program_data_raw = fd_acc_mgr_modify_raw(ctx.global->acc_mgr, ctx.global->funk_txn, programdata_acc, 1, sz2, NULL, NULL, &err);
    if( err != FD_ACC_MGR_SUCCESS ) {
      return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
    }

    if( program_data_raw == NULL ) {
      return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
    }

    fd_account_meta_t * programdata_acc_metadata = (fd_account_meta_t *)program_data_raw;
    uchar * programdata_acc_data = fd_account_get_data(programdata_acc_metadata);
    ulong programdata_data_len = program_acc_metadata->dlen;

    ulong programdata_balance_required = fd_rent_exempt_minimum_balance(ctx.global, programdata_data_len);
    if (programdata_balance_required < 1) {
      programdata_balance_required = 1;
    }

    if (programdata_data_len < fd_ulong_sat_add(PROGRAMDATA_METADATA_SIZE, buffer_data_len)) {
      return FD_EXECUTOR_INSTR_ERR_ACC_DATA_TOO_SMALL;
    }

    if (program_acc_metadata->info.lamports + buffer_acc_metadata->info.lamports < programdata_balance_required) {
      return FD_EXECUTOR_INSTR_ERR_INSUFFICIENT_FUNDS;
    }
    fd_bpf_upgradeable_loader_state_t programdata_loader_state;

    err = 0;
    uchar *ptr = (uchar *) read_bpf_upgradeable_loader_state( ctx.global, programdata_acc, &programdata_loader_state, &err );
    if (NULL == ptr)
      return err;
    if (!fd_bpf_upgradeable_loader_state_is_program_data(&programdata_loader_state)) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ACC_DATA;
    }

    if (FD_FEATURE_ACTIVE(ctx.global, enable_program_redeployment_cooldown) && clock.slot == programdata_loader_state.inner.program_data.slot) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    if (!programdata_loader_state.inner.program_data.upgrade_authority_address) {
      return FD_EXECUTOR_INSTR_ERR_ACC_IMMUTABLE;
    }

    if (memcmp(programdata_loader_state.inner.program_data.upgrade_authority_address, authority_acc, sizeof(fd_pubkey_t)) != 0) {
      return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
    }

    if (instr_acc_idxs[6] >= ctx.txn_ctx->txn_descriptor->signature_cnt) {
      return FD_EXECUTOR_INSTR_ERR_MISSING_REQUIRED_SIGNATURE;
    }

    // TODO: deploy program properly

    err = setup_program(ctx, buffer_acc_data, SIZE_OF_PROGRAM + programdata_data_len);
    if (err != 0) {
      return err;
    }

    sz2 = BUFFER_METADATA_SIZE;
    uchar * buffer_raw_new = fd_acc_mgr_modify_raw( ctx.global->acc_mgr, ctx.global->funk_txn, buffer_acc, 1, sz2, NULL, NULL, &read_result );
    if( FD_UNLIKELY( !buffer_raw_new ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
    }
    fd_account_meta_t * buffer_acc_metadata_new = (fd_account_meta_t *)buffer_raw_new;

    // TODO: min size?
    uchar * spill_raw = fd_acc_mgr_modify_raw( ctx.global->acc_mgr, ctx.global->funk_txn, &txn_accs[instr_acc_idxs[3]], 0, 0, NULL, NULL, &read_result );
    if( FD_UNLIKELY( !spill_raw ) ) {
      FD_LOG_WARNING(( "failed to read account metadata" ));
      return read_result;
    }
    fd_account_meta_t * spill_acc_metadata = (fd_account_meta_t *)spill_raw;

    fd_bpf_upgradeable_loader_state_t program_data_acc_loader_state = {
      .discriminant = fd_bpf_upgradeable_loader_state_enum_program_data,
      .inner.program_data.slot = clock.slot,
      .inner.program_data.upgrade_authority_address = authority_acc,
    };

    fd_bincode_encode_ctx_t encode_ctx = {
      .data = programdata_acc_data,
      .dataend = programdata_acc_data + fd_bpf_upgradeable_loader_state_size(&program_data_acc_loader_state),
    };
    if ( fd_bpf_upgradeable_loader_state_encode( &program_data_acc_loader_state, &encode_ctx ) ) {
      FD_LOG_ERR(("fd_bpf_upgradeable_loader_state_encode failed"));
      fd_memset( programdata_acc_data, 0, fd_bpf_upgradeable_loader_state_size(&program_data_acc_loader_state) );
      return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
    }

    uchar const * buffer_content = buffer_acc_data + BUFFER_METADATA_SIZE;
    uchar * programdata_content = programdata_acc_data + PROGRAMDATA_METADATA_SIZE;
    fd_memcpy(programdata_content, buffer_content, buffer_data_len);
    fd_memset(programdata_content + buffer_data_len, 0, programdata_acc_metadata->dlen-buffer_data_len);

    spill_acc_metadata->info.lamports += programdata_acc_metadata->info.lamports + buffer_lamports - programdata_balance_required;
    buffer_acc_metadata_new->info.lamports = 0;
    programdata_acc_metadata->info.lamports = programdata_balance_required;

    if (FD_FEATURE_ACTIVE(ctx.global, enable_program_redeployment_cooldown)) {
      // TODO: buffer set_data_length
    }

    write_bpf_upgradeable_loader_state( ctx.global, programdata_acc, &program_data_acc_loader_state );
    (void)clock_acc;
    (void)rent_acc;

    return FD_EXECUTOR_INSTR_SUCCESS;

  } else if ( fd_bpf_upgradeable_loader_program_instruction_is_set_authority( &instruction ) ) {
    if( ctx.instr->acct_cnt < 2 ) {
      return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
    }

    fd_pubkey_t * new_authority_acc = NULL;
    if( ctx.instr->acct_cnt >= 3 ) {
      new_authority_acc = &txn_accs[instr_acc_idxs[2]];
    }

    fd_pubkey_t * loader_acc = &txn_accs[instr_acc_idxs[0]];
    fd_pubkey_t * present_authority_acc = &txn_accs[instr_acc_idxs[1]];

    fd_bpf_upgradeable_loader_state_t loader_state;
    int err = 0;
    if (NULL == read_bpf_upgradeable_loader_state( ctx.global, loader_acc, &loader_state, &err)) {
      // FIXME: HANDLE ERRORS!
      return err;
    }

    if( fd_bpf_upgradeable_loader_state_is_buffer( &loader_state ) ) {
      if( new_authority_acc==NULL ) {
        return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
      }

      if( loader_state.inner.buffer.authority_address==NULL ) {
        return FD_EXECUTOR_INSTR_ERR_ACC_IMMUTABLE;
      }

      if ( memcmp( loader_state.inner.buffer.authority_address, present_authority_acc, sizeof(fd_pubkey_t) ) != 0 ) {
        return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
      }

      if(instr_acc_idxs[1] >= ctx.txn_ctx->txn_descriptor->signature_cnt) {
        return FD_EXECUTOR_INSTR_ERR_MISSING_REQUIRED_SIGNATURE;
      }

      loader_state.inner.buffer.authority_address = new_authority_acc;
      return write_bpf_upgradeable_loader_state( ctx.global, loader_acc, &loader_state );
    } else if( fd_bpf_upgradeable_loader_state_is_program_data( &loader_state ) ) {
      if( loader_state.inner.program_data.upgrade_authority_address==NULL ) {
        return FD_EXECUTOR_INSTR_ERR_ACC_IMMUTABLE;
      }

      if ( memcmp( loader_state.inner.program_data.upgrade_authority_address, present_authority_acc, sizeof(fd_pubkey_t) ) != 0 ) {
        return FD_EXECUTOR_INSTR_ERR_INCORRECT_AUTHORITY;
      }

      if(instr_acc_idxs[1] >= ctx.txn_ctx->txn_descriptor->signature_cnt) {
        return FD_EXECUTOR_INSTR_ERR_MISSING_REQUIRED_SIGNATURE;
      }

      loader_state.inner.program_data.upgrade_authority_address = new_authority_acc;

      return write_bpf_upgradeable_loader_state( ctx.global, loader_acc, &loader_state );
    } else {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
  } else if ( fd_bpf_upgradeable_loader_program_instruction_is_close( &instruction ) ) {
    if( ctx.instr->acct_cnt < 2 ) {
      return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
    }

    fd_pubkey_t * close_acc = &txn_accs[instr_acc_idxs[0]];
    fd_pubkey_t * recipient_acc = &txn_accs[instr_acc_idxs[1]];

    if ( memcmp( close_acc, recipient_acc, sizeof(fd_pubkey_t) )==0 ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    fd_bpf_upgradeable_loader_state_t loader_state;
    int err = 0;
    if (NULL == read_bpf_upgradeable_loader_state( ctx.global, close_acc, &loader_state, &err ))
      return err;

    if( fd_bpf_upgradeable_loader_state_is_uninitialized( &loader_state ) ) {
      fd_account_meta_t * close_acc_metadata = NULL;
      int write_result = fd_acc_mgr_modify( ctx.global->acc_mgr, ctx.global->funk_txn, close_acc, 0, 0UL, NULL, NULL, &close_acc_metadata, NULL );
      if( FD_UNLIKELY( write_result != FD_ACC_MGR_SUCCESS ) ) {
        FD_LOG_WARNING(( "failed to read account metadata" ));
        return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
      }

      fd_account_meta_t * recipient_acc_metdata = NULL;
      write_result = fd_acc_mgr_modify( ctx.global->acc_mgr, ctx.global->funk_txn, recipient_acc, 0, 0UL, NULL, NULL, &recipient_acc_metdata, NULL );
      if( FD_UNLIKELY( write_result != FD_ACC_MGR_SUCCESS ) ) {
        FD_LOG_WARNING(( "failed to read account metadata" ));
        return FD_EXECUTOR_INSTR_ERR_MISSING_ACC;
      }

      // FIXME: Do checked addition
      recipient_acc_metdata->info.lamports += close_acc_metadata->info.lamports;
      close_acc_metadata   ->info.lamports = 0;

      return FD_EXECUTOR_INSTR_SUCCESS;
    } else if ( fd_bpf_upgradeable_loader_state_is_buffer( &loader_state ) ) {
      if( ctx.instr->acct_cnt < 3 ) {
        return FD_EXECUTOR_INSTR_ERR_NOT_ENOUGH_ACC_KEYS;
      }

      fd_pubkey_t * authority_acc = &txn_accs[instr_acc_idxs[2]];

      (void)authority_acc;
    } else if( !fd_bpf_upgradeable_loader_state_is_program( &loader_state ) ) {
      return FD_EXECUTOR_INSTR_ERR_INVALID_ARG;
    }

    return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
  } else if ( fd_bpf_upgradeable_loader_program_instruction_is_extend_program( &instruction ) ) {


    return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
  } else {
    FD_LOG_WARNING(( "unsupported bpf upgradeable loader program instruction: discriminant: %d", instruction.discriminant ));
    return FD_EXECUTOR_INSTR_ERR_GENERIC_ERR;
  }
}
