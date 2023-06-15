int test_29(fd_executor_test_suite_t *suite) {
  fd_executor_test_t test;
  fd_memset( &test, 0, FD_EXECUTOR_TEST_FOOTPRINT );
  test.bt = "";
  test.test_name = "secp256k1_instruction::test::test_signature_offset";
  test.test_number = 29;
  if (fd_executor_test_suite_check_filter(suite, &test)) return -9999;

  fd_base58_decode_32( "KeccakSecp256k11111111111111111111111111111",  (unsigned char *) &test.program_id);
  static uchar const fd_flamenco_signer_test_29_raw[] = { 0,0x00,0x00,0x01,0x01,0x04,0xc6,0xfc,0x20,0xf0,0x50,0xcc,0xf0,0x55,0x84,0xd7,0x21,0x1c,0x9f,0x8c,0xf5,0x9e,0xc1,0x47,0x85,0xbb,0x16,0x6a,0x1e,0x28,0x30,0xe8,0x12,0x20,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x00,0x00,0x0c,0x01,0x25,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 };
  test.raw_tx = fd_flamenco_signer_test_29_raw;
  test.raw_tx_len = 85UL;
  test.expected_result = -102;
  test.custom_err = 0;

  return fd_executor_run_test( &test, suite );
}
