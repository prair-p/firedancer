ifneq ($(ROCKSDB),)

ifeq ($(FD_HAS_ZSTD),1)
$(call make-bin,fd_frank_ledger,main tar,fd_util fd_ballet)
$(call make-bin,banks_test,banks_test,fd_util fd_ballet)
endif

else
$(warning ledger tool build disabled due to lack of rocksdb)
endif
