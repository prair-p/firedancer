#ifndef HEADER_fd_src_flamenco_ticketlock_h
#define HEADER_fd_src_flamenco_ticketlock_h

/* A very simple read-write spin lock. */

#include "../util/fd_util_base.h"

struct fd_ticketlock {
  ulong next_ticket;
  ulong curr_ticket;
};

typedef struct fd_ticketlock fd_ticketlock_t;

static inline void
fd_ticketlock_init( fd_ticketlock_t * lock ) {
  lock->next_ticket = 0UL;
  lock->curr_ticket = 0UL;
}

static inline void
fd_ticketlock_lock( fd_ticketlock_t * lock ) {
# if FD_HAS_THREADS
  FD_ATOMIC_
  for(;;) {
    ushort value = lock->value;
    if( FD_LIKELY( !value ) ) {
      if( FD_LIKELY( !FD_ATOMIC_CAS( &lock->value, 0, 0xFFFF ) ) ) return;
    }
    FD_SPIN_PAUSE();
  }
# else
  lock->value = 0xFFFF;
# endif
  FD_COMPILER_MFENCE();
}

static inline void
fd_ticketlock_unlock( fd_ticketlock_t * lock ) {
  FD_COMPILER_MFENCE();
  lock->value = 0;
}

#endif /* HEADER_fd_src_flamenco_ticketlock_h */
