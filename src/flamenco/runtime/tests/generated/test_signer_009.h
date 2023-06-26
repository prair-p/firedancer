int test_9(fd_executor_test_suite_t *suite) {
  fd_executor_test_t test;
  fd_memset( &test, 0, FD_EXECUTOR_TEST_FOOTPRINT );
  test.bt = "";
  test.test_name = "ed25519_instruction::test::test_message_data_offsets";
  test.test_number = 9;
  if (fd_executor_test_suite_check_filter(suite, &test)) return -9999;

  fd_base58_decode_32( "Ed25519SigVerify111111111111111111111111111",  (unsigned char *) &test.program_id);
  static uchar const fd_flamenco_signer_test_9_raw[] = { 0,0x00,0x00,0x01,0x01,0x03,0x7d,0x46,0xd6,0x7c,0x93,0xfb,0xbe,0x12,0xf9,0x42,0x8f,0x83,0x8d,0x40,0xff,0x05,0x70,0x74,0x49,0x27,0xf4,0x8a,0x64,0xfc,0xca,0x70,0x44,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x00,0x00,0x64,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x63,0x00,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 };
  test.raw_tx = fd_flamenco_signer_test_9_raw;
  test.raw_tx_len = 173UL;
  test.expected_result = -102;
  test.custom_err = 0;

  return fd_executor_run_test( &test, suite );
}
