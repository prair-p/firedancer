#ifndef HEADER_fd_src_flamenco_runtime_sysvar_fd_clock_h
#define HEADER_fd_src_flamenco_runtime_sysvar_fd_clock_h

#include "../../fd_flamenco_base.h"
#include "../fd_executor.h"
#include "../../stakes/fd_stake_program.h"

#define SCRATCH_ALIGN     (128UL)
#define SCRATCH_FOOTPRINT (102400UL)
static uchar scratch[ SCRATCH_FOOTPRINT ] __attribute__((aligned(SCRATCH_ALIGN))) __attribute__((used));


#define CIDX_T ulong
#define VAL_T  long
struct ele {
  CIDX_T parent_cidx;
  CIDX_T left_cidx;
  CIDX_T right_cidx;
  CIDX_T prio_cidx;
  VAL_T timestamp;
  unsigned long stake;
};

typedef struct ele ele_t;

#define POOL_NAME  pool
#define POOL_T     ele_t
#define POOL_IDX_T CIDX_T
#define POOL_NEXT  parent_cidx
#include "../../../util/tmpl/fd_pool.c"

FD_FN_CONST static inline int valcmp (VAL_T a, VAL_T b) {
  int val = (a < b) ? -1 : 1;
  return (a == b) ? 0 : val;
}

#define TREAP_NAME       treap
#define TREAP_T          ele_t
#define TREAP_QUERY_T    VAL_T
#define TREAP_CMP(q,e)   valcmp(q, e->timestamp)
#define TREAP_LT(e0,e1)  (((VAL_T)((e0)->timestamp)) < ((VAL_T)((e1)->timestamp)))
#define TREAP_IDX_T      CIDX_T
#define TREAP_PARENT     parent_cidx
#define TREAP_LEFT       left_cidx
#define TREAP_RIGHT      right_cidx
#define TREAP_PRIO       prio_cidx
#define TREAP_IMPL_STYLE 0
#include "../../../util/tmpl/fd_treap.c"

/* The clock sysvar provides an approximate measure of network time. */

/* Initialize the clock sysvar account. */
void fd_sysvar_clock_init( fd_global_ctx_t* global );

/* Update the clock sysvar account. This should be called at the start of every slot, before execution commences. */
int fd_sysvar_clock_update( fd_global_ctx_t* global );

/* Reads the current value of the clock sysvar */
int fd_sysvar_clock_read( fd_global_ctx_t* global, fd_sol_sysvar_clock_t* result );

#endif /* HEADER_fd_src_flamenco_runtime_sysvar_fd_clock_h */
