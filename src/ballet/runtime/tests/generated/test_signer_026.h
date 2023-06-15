int test_26(fd_executor_test_suite_t *suite) {
  fd_executor_test_t test;
  fd_memset( &test, 0, FD_EXECUTOR_TEST_FOOTPRINT );
  test.bt = "";
  test.test_name = "secp256k1_instruction::test::test_secp256k1";
  test.test_number = 26;
  if (fd_executor_test_suite_check_filter(suite, &test)) return -9999;

  fd_base58_decode_32( "KeccakSecp256k11111111111111111111111111111",  (unsigned char *) &test.program_id);
  static uchar const fd_flamenco_signer_test_26_raw[] = { 0,0x00,0x00,0x01,0x01,0x04,0xc6,0xfc,0x20,0xf0,0x50,0xcc,0xf0,0x55,0x84,0xd7,0x21,0x1c,0x9f,0x8c,0xf5,0x9e,0xc1,0x47,0x85,0xbb,0x16,0x6a,0x1e,0x28,0x30,0xe8,0x12,0x20,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x00,0x00,0x66,0x01,0x20,0x00,0x00,0x0c,0x00,0x00,0x61,0x00,0x05,0x00,0x00,0x53,0x04,0xcf,0x68,0x1e,0x6f,0xb0,0x17,0x46,0x7a,0x71,0x18,0xd3,0x9a,0xaf,0xf0,0x43,0x04,0xc9,0x33,0xc8,0x47,0xf6,0x9e,0x4e,0xdb,0xcc,0x36,0xcb,0x25,0x6a,0x3a,0x09,0x55,0x0a,0xc5,0x85,0x31,0x1d,0xb9,0xaf,0x5e,0x8c,0xe5,0xb4,0x79,0x9f,0xb0,0x55,0xd6,0xdf,0xca,0x5e,0xab,0x19,0x30,0xb1,0x6d,0xaf,0x11,0x04,0xc3,0x0d,0x3d,0x91,0x46,0x90,0xf0,0x1f,0x9a,0x7d,0xb6,0xc9,0x5d,0xe4,0x75,0xb4,0x87,0x8b,0xc3,0x91,0x1a,0x84,0x8d,0x00,0x68,0x65,0x6c,0x6c,0x6f };
  test.raw_tx = fd_flamenco_signer_test_26_raw;
  test.raw_tx_len = 175UL;
  test.expected_result = 0;
  test.custom_err = 0;

  return fd_executor_run_test( &test, suite );
}
