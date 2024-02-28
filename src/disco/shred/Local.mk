ifdef FD_HAS_INT128
$(call add-hdrs,fd_fec_resolver.h fd_shred_dest.h fd_shredder.h fd_stake_ci.h)
$(call add-objs,fd_shred_dest fd_shredder fd_fec_resolver fd_stake_ci,fd_disco)
$(call make-unit-test,test_shred_dest,test_shred_dest,fd_ballet fd_util fd_flamenco fd_disco)
$(call make-unit-test,test_shredder,test_shredder,fd_ballet fd_util fd_flamenco fd_disco fd_reedsol)
$(call make-unit-test,test_fec_resolver,test_fec_resolver,fd_ballet fd_util fd_tango fd_flamenco fd_disco fd_reedsol)
$(call make-unit-test,test_stake_ci,test_stake_ci,fd_ballet fd_util fd_tango fd_flamenco fd_disco fd_reedsol)
$(call run-unit-test,test_shred_dest,)
$(call run-unit-test,test_shredder,)
$(call run-unit-test,test_fec_resolver,)
$(call run-unit-test,test_stake_ci,)
endif
