ifdef FD_HAS_OPENSSL

$(call make-bin,test_quic_hs,test_quic_hs,fd_aio fd_quic fd_util)
$(call make-bin,test_handshake,test_handshake,fd_aio fd_quic fd_util)
$(call make-bin,test_crypto,test_crypto,fd_quic fd_util)
$(call make-bin,test_frames,test_frames,fd_quic fd_util)
$(call make-bin,test_checksum,test_checksum,fd_util)
$(call make-bin,test_tls_decrypt,test_tls_decrypt,fd_quic fd_util)
$(call make-bin,dump_struct_sizes,dump_struct_sizes,)

endif
