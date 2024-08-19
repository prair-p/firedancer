ifdef FD_HAS_HOSTED
$(call add-hdrs,fd_quic_sandbox.h fd_quic_test_helpers.h)
$(call add-objs,fd_quic_stream_spam fd_quic_sandbox fd_quic_test_helpers,fd_quic)

# fd_quic unit tests
$(call make-unit-test,test_quic_hs,         test_quic_hs,         fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_streams,    test_quic_streams,    fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_conn,       test_quic_conn,       fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_drops,      test_quic_drops,      fd_quic fd_tls fd_ballet fd_waltz fd_util fd_fibre)
$(call make-unit-test,test_quic_slowloris,  test_quic_slowloris,  fd_quic fd_tls fd_ballet fd_waltz fd_util fd_fibre)
$(call make-unit-test,test_quic_bw,         test_quic_bw,         fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_layout,     test_quic_layout,                                       fd_util)
$(call make-unit-test,test_quic_conformance,test_quic_conformance,fd_quic fd_tls fd_tango fd_ballet fd_waltz fd_util)
# $(call run-unit-test,test_quic_hs)
$(call run-unit-test,test_quic_streams)
#$(call run-unit-test,test_quic_conn) -- broken because of fd_ip
#$(call run-unit-test,test_quic_bw) -- broken because of fd_ip
$(call run-unit-test,test_quic_layout)

# fd_quic_tls unit tests
$(call make-unit-test,test_quic_tls_hs,test_quic_tls_hs,fd_quic fd_tls fd_ballet fd_util)
$(call run-unit-test,test_quic_tls_hs)

# fd_quic_crypto unit tests
$(call make-unit-test,test_quic_crypto,test_quic_crypto,fd_quic fd_tls fd_ballet fd_util)
$(call run-unit-test,test_quic_crypto)

# Manual test programs
$(call make-unit-test,test_quic_client_flood,test_quic_client_flood,fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_server,test_quic_server,            fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_txns,  test_quic_txns,              fd_quic fd_tls fd_ballet fd_waltz fd_util)

$(call make-unit-test,test_quic_frames,test_frames,fd_quic fd_util)
$(call make-unit-test,test_quic_retry_unit,test_quic_retry_unit,fd_quic fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_retry_integration,test_quic_retry_integration,fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_arp_server,arp/test_quic_arp_server,fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-unit-test,test_quic_arp_client,arp/test_quic_arp_client,fd_quic fd_tls fd_ballet fd_waltz fd_util fd_fibre)
$(call run-unit-test,test_quic_retry_unit)
$(call run-unit-test,test_quic_retry_integration)

ifdef FD_HAS_HOSTED
$(call make-fuzz-test,fuzz_quic,fuzz_quic,fd_quic fd_tls fd_ballet fd_waltz fd_util)
$(call make-fuzz-test,fuzz_quic_wire,fuzz_quic_wire,fd_quic fd_tls fd_ballet fd_waltz fd_util)
endif

$(call run-unit-test,test_quic_frames)
endif
