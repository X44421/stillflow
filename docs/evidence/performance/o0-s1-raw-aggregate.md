# O0-S1 storage cost probe — aggregated over 5 run(s)

- info: {"kind": "info", "machine": {"cpu_count": 6, "cpu_model": "12th Gen Intel(R) Core(TM) i3-12100F", "kernel": "Linux version 6.18.33.2-microsoft-standard-WSL2 (root@f1bbfb02316b) (gcc (GCC) 13.2.0, GNU ld (GNU Binutils) 2.41) #1 SMP PREEMPT_DYNAMIC Thu Jun 18 21:54:43 UTC 2026", "mem_total_kib": 12249204, "os": "linux"}}
- info: {"buffer_bytes": 65536, "calibration": "sha256", "kind": "info", "note": "pure-CPU SHA-256 over in-memory 64 KiB chunks, same chunk size as digest_file", "ns_per_byte": 0.5669519156217575}
- info: {"kind": "info", "machine": {"cpu_count": 6, "cpu_model": "12th Gen Intel(R) Core(TM) i3-12100F", "kernel": "Linux version 6.18.33.2-microsoft-standard-WSL2 (root@f1bbfb02316b) (gcc (GCC) 13.2.0, GNU ld (GNU Binutils) 2.41) #1 SMP PREEMPT_DYNAMIC Thu Jun 18 21:54:43 UTC 2026", "mem_total_kib": 12249204, "os": "linux"}}
- info: {"buffer_bytes": 65536, "calibration": "sha256", "kind": "info", "note": "pure-CPU SHA-256 over in-memory 64 KiB chunks, same chunk size as digest_file", "ns_per_byte": 0.5282550901174545}
- info: {"kind": "info", "machine": {"cpu_count": 6, "cpu_model": "12th Gen Intel(R) Core(TM) i3-12100F", "kernel": "Linux version 6.18.33.2-microsoft-standard-WSL2 (root@f1bbfb02316b) (gcc (GCC) 13.2.0, GNU ld (GNU Binutils) 2.41) #1 SMP PREEMPT_DYNAMIC Thu Jun 18 21:54:43 UTC 2026", "mem_total_kib": 12249204, "os": "linux"}}
- info: {"buffer_bytes": 65536, "calibration": "sha256", "kind": "info", "note": "pure-CPU SHA-256 over in-memory 64 KiB chunks, same chunk size as digest_file", "ns_per_byte": 0.5319982618093491}
- info: {"kind": "info", "machine": {"cpu_count": 6, "cpu_model": "12th Gen Intel(R) Core(TM) i3-12100F", "kernel": "Linux version 6.18.33.2-microsoft-standard-WSL2 (root@f1bbfb02316b) (gcc (GCC) 13.2.0, GNU ld (GNU Binutils) 2.41) #1 SMP PREEMPT_DYNAMIC Thu Jun 18 21:54:43 UTC 2026", "mem_total_kib": 12249204, "os": "linux"}}
- info: {"buffer_bytes": 65536, "calibration": "sha256", "kind": "info", "note": "pure-CPU SHA-256 over in-memory 64 KiB chunks, same chunk size as digest_file", "ns_per_byte": 0.532984733581543}
- info: {"kind": "info", "machine": {"cpu_count": 6, "cpu_model": "12th Gen Intel(R) Core(TM) i3-12100F", "kernel": "Linux version 6.18.33.2-microsoft-standard-WSL2 (root@f1bbfb02316b) (gcc (GCC) 13.2.0, GNU ld (GNU Binutils) 2.41) #1 SMP PREEMPT_DYNAMIC Thu Jun 18 21:54:43 UTC 2026", "mem_total_kib": 12249204, "os": "linux"}}
- info: {"buffer_bytes": 65536, "calibration": "sha256", "kind": "info", "note": "pure-CPU SHA-256 over in-memory 64 KiB chunks, same chunk size as digest_file", "ns_per_byte": 0.5335375517606735}

## a.fail.drop

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| conn_open_configure_ns | 5 | 3,415,199 (3,415.2 us) | 3,510,100 (3,510.1 us) | 3,216,900 (3,216.9 us) | 3,510,100 (3,510.1 us) | 8.6% |
| conn_open_count | 5 | 7 | 7 | 7 | 7 | 0.0% |
| conn_open_open_ns | 5 | 287,500 (287.5 us) | 310,300 (310.3 us) | 253,100 (253.1 us) | 310,300 (310.3 us) | 19.9% |
| conn_open_pragma_count | 5 | 21 | 21 | 21 | 21 | 0.0% |
| db_op_publication_abort_commit_ns | 5 | 0 | 0 | 0 | 0 | n/a |
| db_op_publication_abort_count | 5 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_abort_open_ns | 5 | 605,300 (605.3 us) | 857,600 (857.6 us) | 581,800 (581.8 us) | 857,600 (857.6 us) | 45.6% |
| db_op_publication_abort_opens | 5 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_abort_stmt_ns | 5 | 42,500 (42.5 us) | 115,000 (115.0 us) | 38,400 (38.4 us) | 115,000 (115.0 us) | 180.2% |
| db_op_publication_abort_txn_begin_ns | 5 | 0 | 0 | 0 | 0 | n/a |
| db_op_publication_abort_wall_ns | 5 | 645,100 (645.1 us) | 972,700 (972.7 us) | 620,500 (620.5 us) | 972,700 (972.7 us) | 54.6% |
| db_op_publication_journal_commit_ns | 5 | 15,800 (15.8 us) | 21,900 (21.9 us) | 14,300 (14.3 us) | 21,900 (21.9 us) | 48.1% |
| db_op_publication_journal_count | 5 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_open_ns | 5 | 485,900 (485.9 us) | 606,800 (606.8 us) | 470,100 (470.1 us) | 606,800 (606.8 us) | 28.1% |
| db_op_publication_journal_opens | 5 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_stmt_ns | 5 | 62,600 (62.6 us) | 71,700 (71.7 us) | 59,800 (59.8 us) | 71,700 (71.7 us) | 19.0% |
| db_op_publication_journal_txn_begin_ns | 5 | 2,600 (2.6 us) | 3,800 (3.8 us) | 2,300 (2.3 us) | 3,800 (3.8 us) | 57.7% |
| db_op_publication_journal_wall_ns | 5 | 564,500 (564.5 us) | 704,400 (704.4 us) | 549,700 (549.7 us) | 704,400 (704.4 us) | 27.4% |
| parquet_bytes_written | 5 | 2,781,937 | 2,781,937 | 2,781,937 | 2,781,937 | 0.0% |
| parquet_create_ns | 5 | 3,800 (3.8 us) | 5,400 (5.4 us) | 3,200 (3.2 us) | 5,400 (5.4 us) | 57.9% |
| parquet_digest_ns | 5 | 1,839,200 (1,839.2 us) | 2,379,000 (2,379.0 us) | 1,716,800 (1,716.8 us) | 2,379,000 (2,379.0 us) | 36.0% |
| parquet_encode_ns | 5 | 21,407,100 (21,407.1 us) | 22,620,399 (22,620.4 us) | 19,121,699 (19,121.7 us) | 22,620,399 (22,620.4 us) | 16.3% |
| parquet_fsync_ns | 5 | 1,500 (1.5 us) | 1,600 (1.6 us) | 1,200 (1.2 us) | 1,600 (1.6 us) | 26.7% |
| parquet_reread_bytes | 5 | 2,781,937 | 2,781,937 | 2,781,937 | 2,781,937 | 0.0% |
| parquet_reread_passes | 5 | 1 | 1 | 1 | 1 | 0.0% |
| parquet_rewind_ns | 5 | 1,000 (1.0 us) | 1,600 (1.6 us) | 800 | 1,600 (1.6 us) | 80.0% |
| parquet_stat_ns | 5 | 3,500 (3.5 us) | 3,900 (3.9 us) | 3,200 (3.2 us) | 3,900 (3.9 us) | 20.0% |
| parquet_total_ns | 5 | 23,376,800 (23,376.8 us) | 24,418,399 (24,418.4 us) | 20,848,899 (20,848.9 us) | 24,418,399 (24,418.4 us) | 15.3% |
| parquet_write_count | 5 | 1 | 1 | 1 | 1 | 0.0% |
| vm_hwm_kib | 5 | 201,504 | 201,732 | 197,928 | 201,732 | 1.9% |
| wall_ns | 5 | 25,442,700 (25,442.7 us) | 26,181,899 (26,181.9 us) | 22,396,299 (22,396.3 us) | 26,181,899 (26,181.9 us) | 14.9% |

## a.write.longvar

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| conn_open_configure_ns | 35 | 1,138,000 (1,138.0 us) | 1,567,600 (1,567.6 us) | 940,500 (940.5 us) | 1,714,500 (1,714.5 us) | 38.2% |
| conn_open_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| conn_open_open_ns | 35 | 84,400 (84.4 us) | 145,900 (145.9 us) | 72,000 (72.0 us) | 165,100 (165.1 us) | 22.0% |
| conn_open_pragma_count | 35 | 6 | 6 | 6 | 6 | 0.0% |
| db_op_manifest_commit_commit_ns | 35 | 35,100 (35.1 us) | 74,100 (74.1 us) | 25,200 (25.2 us) | 88,300 (88.3 us) | 31.1% |
| db_op_manifest_commit_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_open_ns | 35 | 639,400 (639.4 us) | 1,053,400 (1,053.4 us) | 553,100 (553.1 us) | 1,153,000 (1,153.0 us) | 40.3% |
| db_op_manifest_commit_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_stmt_ns | 35 | 94,500 (94.5 us) | 151,900 (151.9 us) | 76,600 (76.6 us) | 160,200 (160.2 us) | 37.1% |
| db_op_manifest_commit_txn_begin_ns | 35 | 4,000 (4.0 us) | 8,300 (8.3 us) | 3,100 (3.1 us) | 11,800 (11.8 us) | 65.0% |
| db_op_manifest_commit_wall_ns | 35 | 770,400 (770.4 us) | 1,216,200 (1,216.2 us) | 664,400 (664.4 us) | 1,348,200 (1,348.2 us) | 38.2% |
| db_op_publication_journal_commit_ns | 35 | 15,500 (15.5 us) | 38,900 (38.9 us) | 13,500 (13.5 us) | 102,500 (102.5 us) | 80.6% |
| db_op_publication_journal_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_open_ns | 35 | 477,200 (477.2 us) | 797,400 (797.4 us) | 436,600 (436.6 us) | 1,055,800 (1,055.8 us) | 53.0% |
| db_op_publication_journal_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_stmt_ns | 35 | 60,500 (60.5 us) | 116,600 (116.6 us) | 55,500 (55.5 us) | 436,500 (436.5 us) | 40.6% |
| db_op_publication_journal_txn_begin_ns | 35 | 2,800 (2.8 us) | 5,600 (5.6 us) | 2,300 (2.3 us) | 14,100 (14.1 us) | 67.9% |
| db_op_publication_journal_wall_ns | 35 | 561,200 (561.2 us) | 921,400 (921.4 us) | 510,500 (510.5 us) | 1,609,100 (1,609.1 us) | 53.2% |
| install_dir_fsync_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| install_dir_fsync_ns | 35 | 7,700 (7.7 us) | 27,900 (27.9 us) | 6,100 (6.1 us) | 68,600 (68.6 us) | 45.9% |
| install_rename_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| install_rename_ns | 35 | 11,000 (11.0 us) | 25,300 (25.3 us) | 8,300 (8.3 us) | 31,300 (31.3 us) | 29.9% |
| install_wall_ns | 35 | 23,000 (23.0 us) | 72,100 (72.1 us) | 18,200 (18.2 us) | 84,800 (84.8 us) | 35.8% |
| parquet_bytes_written | 35 | 2,075,930 | 2,075,930 | 2,075,930 | 2,075,930 | 0.0% |
| parquet_create_ns | 35 | 4,000 (4.0 us) | 6,800 (6.8 us) | 3,000 (3.0 us) | 8,100 (8.1 us) | 25.6% |
| parquet_digest_ns | 35 | 1,401,500 (1,401.5 us) | 1,631,500 (1,631.5 us) | 1,264,600 (1,264.6 us) | 1,672,000 (1,672.0 us) | 17.0% |
| parquet_encode_ns | 35 | 29,096,600 (29,096.6 us) | 45,272,399 (45,272.4 us) | 26,572,700 (26,572.7 us) | 45,532,999 (45,533.0 us) | 34.0% |
| parquet_fsync_ns | 35 | 1,600 (1.6 us) | 3,200 (3.2 us) | 900 | 3,400 (3.4 us) | 20.0% |
| parquet_reread_bytes | 35 | 2,075,930 | 2,075,930 | 2,075,930 | 2,075,930 | 0.0% |
| parquet_reread_passes | 35 | 1 | 1 | 1 | 1 | 0.0% |
| parquet_rewind_ns | 35 | 1,000 (1.0 us) | 2,200 (2.2 us) | 600 | 2,400 (2.4 us) | 50.0% |
| parquet_stat_ns | 35 | 3,800 (3.8 us) | 6,200 (6.2 us) | 2,700 (2.7 us) | 6,200 (6.2 us) | 36.8% |
| parquet_total_ns | 35 | 30,443,600 (30,443.6 us) | 46,963,599 (46,963.6 us) | 28,125,400 (28,125.4 us) | 47,115,199 (47,115.2 us) | 33.2% |
| parquet_write_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| partition_install_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| vm_hwm_kib | 35 | 95,632 | 95,872 | 95,556 | 95,872 | 0.3% |
| wall_ns | 35 | 32,213,000 (32,213.0 us) | 49,455,599 (49,455.6 us) | 30,074,200 (30,074.2 us) | 49,951,699 (49,951.7 us) | 33.4% |

## a.write.medium

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| conn_open_configure_ns | 35 | 1,017,600 (1,017.6 us) | 1,441,400 (1,441.4 us) | 938,000 (938.0 us) | 1,453,400 (1,453.4 us) | 27.2% |
| conn_open_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| conn_open_open_ns | 35 | 83,100 (83.1 us) | 139,600 (139.6 us) | 71,500 (71.5 us) | 241,000 (241.0 us) | 37.4% |
| conn_open_pragma_count | 35 | 6 | 6 | 6 | 6 | 0.0% |
| db_op_manifest_commit_commit_ns | 35 | 32,900 (32.9 us) | 69,500 (69.5 us) | 24,800 (24.8 us) | 145,700 (145.7 us) | 25.1% |
| db_op_manifest_commit_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_open_ns | 35 | 597,600 (597.6 us) | 949,800 (949.8 us) | 551,900 (551.9 us) | 980,100 (980.1 us) | 14.2% |
| db_op_manifest_commit_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_stmt_ns | 35 | 87,800 (87.8 us) | 147,800 (147.8 us) | 79,400 (79.4 us) | 195,100 (195.1 us) | 9.9% |
| db_op_manifest_commit_txn_begin_ns | 35 | 3,500 (3.5 us) | 8,000 (8.0 us) | 3,000 (3.0 us) | 8,400 (8.4 us) | 22.9% |
| db_op_manifest_commit_wall_ns | 35 | 735,600 (735.6 us) | 1,126,200 (1,126.2 us) | 670,900 (670.9 us) | 1,146,300 (1,146.3 us) | 9.7% |
| db_op_publication_journal_commit_ns | 35 | 15,900 (15.9 us) | 27,500 (27.5 us) | 12,300 (12.3 us) | 48,000 (48.0 us) | 26.8% |
| db_op_publication_journal_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_open_ns | 35 | 477,700 (477.7 us) | 743,000 (743.0 us) | 425,700 (425.7 us) | 812,700 (812.7 us) | 37.7% |
| db_op_publication_journal_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_stmt_ns | 35 | 61,200 (61.2 us) | 115,000 (115.0 us) | 54,900 (54.9 us) | 118,600 (118.6 us) | 28.3% |
| db_op_publication_journal_txn_begin_ns | 35 | 2,800 (2.8 us) | 4,700 (4.7 us) | 2,400 (2.4 us) | 5,900 (5.9 us) | 29.6% |
| db_op_publication_journal_wall_ns | 35 | 558,500 (558.5 us) | 867,600 (867.6 us) | 497,000 (497.0 us) | 981,900 (981.9 us) | 37.1% |
| install_dir_fsync_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| install_dir_fsync_ns | 35 | 7,600 (7.6 us) | 16,200 (16.2 us) | 6,300 (6.3 us) | 16,400 (16.4 us) | 16.9% |
| install_rename_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| install_rename_ns | 35 | 10,300 (10.3 us) | 18,900 (18.9 us) | 8,800 (8.8 us) | 19,600 (19.6 us) | 8.7% |
| install_wall_ns | 35 | 22,400 (22.4 us) | 40,500 (40.5 us) | 19,900 (19.9 us) | 41,700 (41.7 us) | 13.8% |
| parquet_bytes_written | 35 | 2,781,937 | 2,781,937 | 2,781,937 | 2,781,937 | 0.0% |
| parquet_create_ns | 35 | 4,000 (4.0 us) | 6,200 (6.2 us) | 3,000 (3.0 us) | 8,200 (8.2 us) | 41.0% |
| parquet_digest_ns | 35 | 1,955,900 (1,955.9 us) | 2,269,800 (2,269.8 us) | 1,828,700 (1,828.7 us) | 2,312,300 (2,312.3 us) | 7.9% |
| parquet_encode_ns | 35 | 21,370,700 (21,370.7 us) | 28,899,600 (28,899.6 us) | 19,448,099 (19,448.1 us) | 59,853,699 (59,853.7 us) | 29.0% |
| parquet_fsync_ns | 35 | 1,500 (1.5 us) | 2,000 (2.0 us) | 1,100 (1.1 us) | 2,000 (2.0 us) | 20.0% |
| parquet_reread_bytes | 35 | 2,781,937 | 2,781,937 | 2,781,937 | 2,781,937 | 0.0% |
| parquet_reread_passes | 35 | 1 | 1 | 1 | 1 | 0.0% |
| parquet_rewind_ns | 35 | 1,000 (1.0 us) | 1,500 (1.5 us) | 700 | 1,600 (1.6 us) | 20.0% |
| parquet_stat_ns | 35 | 3,800 (3.8 us) | 5,500 (5.5 us) | 3,100 (3.1 us) | 7,200 (7.2 us) | 13.5% |
| parquet_total_ns | 35 | 23,366,600 (23,366.6 us) | 30,867,500 (30,867.5 us) | 21,601,100 (21,601.1 us) | 62,138,299 (62,138.3 us) | 26.9% |
| parquet_write_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| partition_install_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| vm_hwm_kib | 35 | 95,632 | 95,872 | 95,556 | 95,872 | 0.3% |
| wall_ns | 35 | 25,130,300 (25,130.3 us) | 33,088,400 (33,088.4 us) | 23,243,900 (23,243.9 us) | 64,381,799 (64,381.8 us) | 25.6% |

## a.write.small

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| conn_open_configure_ns | 35 | 867,900 (867.9 us) | 1,344,800 (1,344.8 us) | 779,400 (779.4 us) | 1,547,200 (1,547.2 us) | 36.4% |
| conn_open_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| conn_open_open_ns | 35 | 49,200 (49.2 us) | 80,400 (80.4 us) | 43,200 (43.2 us) | 85,000 (85.0 us) | 51.9% |
| conn_open_pragma_count | 35 | 6 | 6 | 6 | 6 | 0.0% |
| db_op_manifest_commit_commit_ns | 35 | 26,000 (26.0 us) | 51,500 (51.5 us) | 20,500 (20.5 us) | 72,300 (72.3 us) | 69.6% |
| db_op_manifest_commit_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_open_ns | 35 | 462,500 (462.5 us) | 861,600 (861.6 us) | 421,600 (421.6 us) | 912,600 (912.6 us) | 25.7% |
| db_op_manifest_commit_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_stmt_ns | 35 | 60,100 (60.1 us) | 139,900 (139.9 us) | 53,900 (53.9 us) | 141,600 (141.6 us) | 33.3% |
| db_op_manifest_commit_txn_begin_ns | 35 | 2,800 (2.8 us) | 5,900 (5.9 us) | 2,300 (2.3 us) | 6,000 (6.0 us) | 60.7% |
| db_op_manifest_commit_wall_ns | 35 | 548,000 (548.0 us) | 1,015,700 (1,015.7 us) | 512,700 (512.7 us) | 1,060,200 (1,060.2 us) | 27.0% |
| db_op_publication_journal_commit_ns | 35 | 15,000 (15.0 us) | 27,300 (27.3 us) | 12,300 (12.3 us) | 29,600 (29.6 us) | 36.2% |
| db_op_publication_journal_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_open_ns | 35 | 446,700 (446.7 us) | 711,900 (711.9 us) | 400,300 (400.3 us) | 716,400 (716.4 us) | 15.3% |
| db_op_publication_journal_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_stmt_ns | 35 | 54,100 (54.1 us) | 88,200 (88.2 us) | 48,200 (48.2 us) | 97,900 (97.9 us) | 58.6% |
| db_op_publication_journal_txn_begin_ns | 35 | 2,500 (2.5 us) | 4,700 (4.7 us) | 2,100 (2.1 us) | 5,700 (5.7 us) | 18.5% |
| db_op_publication_journal_wall_ns | 35 | 521,200 (521.2 us) | 839,400 (839.4 us) | 464,500 (464.5 us) | 840,300 (840.3 us) | 15.5% |
| install_dir_fsync_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| install_dir_fsync_ns | 35 | 3,000 (3.0 us) | 5,800 (5.8 us) | 2,700 (2.7 us) | 10,700 (10.7 us) | 51.7% |
| install_rename_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| install_rename_ns | 35 | 6,400 (6.4 us) | 14,400 (14.4 us) | 4,800 (4.8 us) | 18,400 (18.4 us) | 61.8% |
| install_wall_ns | 35 | 11,900 (11.9 us) | 24,300 (24.3 us) | 9,800 (9.8 us) | 71,800 (71.8 us) | 69.4% |
| parquet_bytes_written | 35 | 19,031 | 19,031 | 19,031 | 19,031 | 0.0% |
| parquet_create_ns | 35 | 3,900 (3.9 us) | 7,800 (7.8 us) | 2,900 (2.9 us) | 23,100 (23.1 us) | 61.0% |
| parquet_digest_ns | 35 | 14,600 (14.6 us) | 23,800 (23.8 us) | 13,100 (13.1 us) | 38,000 (38.0 us) | 15.2% |
| parquet_encode_ns | 35 | 223,900 (223.9 us) | 392,200 (392.2 us) | 190,100 (190.1 us) | 503,000 (503.0 us) | 29.7% |
| parquet_fsync_ns | 35 | 400 | 600 | 200 | 800 | 50.0% |
| parquet_reread_bytes | 35 | 19,031 | 19,031 | 19,031 | 19,031 | 0.0% |
| parquet_reread_passes | 35 | 1 | 1 | 1 | 1 | 0.0% |
| parquet_rewind_ns | 35 | 500 | 1,000 (1.0 us) | 300 | 1,100 (1.1 us) | 75.0% |
| parquet_stat_ns | 35 | 800 | 1,800 (1.8 us) | 500 | 2,100 (2.1 us) | 42.9% |
| parquet_total_ns | 35 | 250,200 (250.2 us) | 447,800 (447.8 us) | 208,800 (208.8 us) | 543,900 (543.9 us) | 27.8% |
| parquet_write_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| partition_install_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| vm_hwm_kib | 35 | 94,788 | 95,040 | 94,628 | 95,040 | 0.4% |
| wall_ns | 35 | 1,608,000 (1,608.0 us) | 2,588,600 (2,588.6 us) | 1,403,200 (1,403.2 us) | 2,648,800 (2,648.8 us) | 42.3% |

## a.write.wide

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| conn_open_configure_ns | 35 | 983,600 (983.6 us) | 1,347,400 (1,347.4 us) | 902,300 (902.3 us) | 1,588,200 (1,588.2 us) | 13.7% |
| conn_open_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| conn_open_open_ns | 35 | 77,100 (77.1 us) | 174,800 (174.8 us) | 68,800 (68.8 us) | 444,000 (444.0 us) | 22.8% |
| conn_open_pragma_count | 35 | 6 | 6 | 6 | 6 | 0.0% |
| db_op_manifest_commit_commit_ns | 35 | 43,700 (43.7 us) | 94,500 (94.5 us) | 37,800 (37.8 us) | 98,000 (98.0 us) | 47.7% |
| db_op_manifest_commit_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_open_ns | 35 | 574,700 (574.7 us) | 984,800 (984.8 us) | 543,100 (543.1 us) | 1,168,900 (1,168.9 us) | 5.1% |
| db_op_manifest_commit_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_stmt_ns | 35 | 111,600 (111.6 us) | 208,400 (208.4 us) | 104,800 (104.8 us) | 247,200 (247.2 us) | 19.3% |
| db_op_manifest_commit_txn_begin_ns | 35 | 3,700 (3.7 us) | 6,200 (6.2 us) | 2,900 (2.9 us) | 7,200 (7.2 us) | 10.8% |
| db_op_manifest_commit_wall_ns | 35 | 737,000 (737.0 us) | 1,231,900 (1,231.9 us) | 697,100 (697.1 us) | 1,367,600 (1,367.6 us) | 3.8% |
| db_op_publication_journal_commit_ns | 35 | 15,500 (15.5 us) | 35,500 (35.5 us) | 12,800 (12.8 us) | 38,100 (38.1 us) | 7.7% |
| db_op_publication_journal_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_open_ns | 35 | 481,700 (481.7 us) | 880,600 (880.6 us) | 439,600 (439.6 us) | 961,300 (961.3 us) | 8.0% |
| db_op_publication_journal_opens | 35 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_stmt_ns | 35 | 58,400 (58.4 us) | 98,400 (98.4 us) | 56,100 (56.1 us) | 109,600 (109.6 us) | 5.8% |
| db_op_publication_journal_txn_begin_ns | 35 | 2,800 (2.8 us) | 5,400 (5.4 us) | 2,300 (2.3 us) | 5,900 (5.9 us) | 14.3% |
| db_op_publication_journal_wall_ns | 35 | 560,800 (560.8 us) | 1,031,800 (1,031.8 us) | 513,800 (513.8 us) | 1,099,300 (1,099.3 us) | 7.3% |
| install_dir_fsync_count | 35 | 2 | 2 | 2 | 2 | 0.0% |
| install_dir_fsync_ns | 35 | 7,600 (7.6 us) | 12,200 (12.2 us) | 6,400 (6.4 us) | 13,300 (13.3 us) | 32.0% |
| install_rename_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| install_rename_ns | 35 | 10,500 (10.5 us) | 17,400 (17.4 us) | 8,400 (8.4 us) | 17,500 (17.5 us) | 9.5% |
| install_wall_ns | 35 | 21,900 (21.9 us) | 37,400 (37.4 us) | 18,900 (18.9 us) | 38,200 (38.2 us) | 22.2% |
| parquet_bytes_written | 35 | 1,695,288 | 1,695,288 | 1,695,288 | 1,695,288 | 0.0% |
| parquet_create_ns | 35 | 4,100 (4.1 us) | 6,700 (6.7 us) | 3,000 (3.0 us) | 7,200 (7.2 us) | 22.0% |
| parquet_digest_ns | 35 | 1,109,100 (1,109.1 us) | 1,265,200 (1,265.2 us) | 1,020,800 (1,020.8 us) | 1,381,300 (1,381.3 us) | 8.7% |
| parquet_encode_ns | 35 | 13,246,700 (13,246.7 us) | 16,273,199 (16,273.2 us) | 12,519,200 (12,519.2 us) | 18,305,000 (18,305.0 us) | 12.4% |
| parquet_fsync_ns | 35 | 1,400 (1.4 us) | 1,700 (1.7 us) | 1,000 (1.0 us) | 1,700 (1.7 us) | 7.7% |
| parquet_reread_bytes | 35 | 1,695,288 | 1,695,288 | 1,695,288 | 1,695,288 | 0.0% |
| parquet_reread_passes | 35 | 1 | 1 | 1 | 1 | 0.0% |
| parquet_rewind_ns | 35 | 900 | 1,300 (1.3 us) | 700 | 1,900 (1.9 us) | 22.2% |
| parquet_stat_ns | 35 | 3,500 (3.5 us) | 4,400 (4.4 us) | 2,900 (2.9 us) | 5,600 (5.6 us) | 17.1% |
| parquet_total_ns | 35 | 14,557,399 (14,557.4 us) | 17,534,599 (17,534.6 us) | 13,637,999 (13,638.0 us) | 19,548,500 (19,548.5 us) | 11.5% |
| parquet_write_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| partition_install_count | 35 | 1 | 1 | 1 | 1 | 0.0% |
| vm_hwm_kib | 35 | 95,632 | 95,872 | 95,556 | 95,872 | 0.3% |
| wall_ns | 35 | 16,771,500 (16,771.5 us) | 20,856,099 (20,856.1 us) | 15,555,199 (15,555.2 us) | 21,459,000 (21,459.0 us) | 12.9% |

## b.conc.mixed

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| publisher_op_ns | 1033 | 14,094,699 (14,094.7 us) | 18,674,400 (18,674.4 us) | 3,444,400 (3,444.4 us) | 41,269,700 (41,269.7 us) | 12.7% |
| reader_op_ns | 4529 | 6,460,400 (6,460.4 us) | 8,887,900 (8,887.9 us) | 491,000 (491.0 us) | 25,196,699 (25,196.7 us) | 10.8% |

## b.op.create_dataset

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| conn_open_configure_ns | 150 | 400,099 (400.1 us) | 589,600 (589.6 us) | 369,100 (369.1 us) | 871,900 (871.9 us) | 5.3% |
| conn_open_count | 150 | 1 | 1 | 1 | 1 | 0.0% |
| conn_open_open_ns | 150 | 36,800 (36.8 us) | 53,000 (53.0 us) | 34,700 (34.7 us) | 109,700 (109.7 us) | 5.4% |
| conn_open_pragma_count | 150 | 3 | 3 | 3 | 3 | 0.0% |
| db_op_create_dataset_commit_ns | 150 | 0 | 0 | 0 | 0 | n/a |
| db_op_create_dataset_count | 150 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_create_dataset_open_ns | 150 | 441,300 (441.3 us) | 629,300 (629.3 us) | 406,300 (406.3 us) | 941,200 (941.2 us) | 6.2% |
| db_op_create_dataset_opens | 150 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_create_dataset_stmt_ns | 150 | 59,600 (59.6 us) | 101,700 (101.7 us) | 50,800 (50.8 us) | 271,200 (271.2 us) | 12.7% |
| db_op_create_dataset_txn_begin_ns | 150 | 0 | 0 | 0 | 0 | n/a |
| db_op_create_dataset_wall_ns | 150 | 504,200 (504.2 us) | 714,800 (714.8 us) | 460,700 (460.7 us) | 1,212,600 (1,212.6 us) | 6.2% |
| wall_ns | 150 | 569,800 (569.8 us) | 794,500 (794.5 us) | 524,200 (524.2 us) | 1,326,000 (1,326.0 us) | 6.7% |

## b.op.load_manifest

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| conn_open_configure_ns | 150 | 403,600 (403.6 us) | 633,600 (633.6 us) | 364,900 (364.9 us) | 1,192,500 (1,192.5 us) | 6.6% |
| conn_open_count | 150 | 1 | 1 | 1 | 1 | 0.0% |
| conn_open_open_ns | 150 | 36,600 (36.6 us) | 58,800 (58.8 us) | 19,800 (19.8 us) | 116,700 (116.7 us) | 4.6% |
| conn_open_pragma_count | 150 | 3 | 3 | 3 | 3 | 0.0% |
| db_op_load_manifest_commit_ns | 150 | 0 | 0 | 0 | 0 | n/a |
| db_op_load_manifest_count | 150 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_load_manifest_open_ns | 150 | 441,700 (441.7 us) | 677,700 (677.7 us) | 401,700 (401.7 us) | 1,231,800 (1,231.8 us) | 6.2% |
| db_op_load_manifest_opens | 150 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_load_manifest_stmt_ns | 150 | 42,000 (42.0 us) | 105,700 (105.7 us) | 36,000 (36.0 us) | 190,400 (190.4 us) | 6.7% |
| db_op_load_manifest_txn_begin_ns | 150 | 0 | 0 | 0 | 0 | n/a |
| db_op_load_manifest_wall_ns | 150 | 484,100 (484.1 us) | 780,200 (780.2 us) | 437,800 (437.8 us) | 1,373,100 (1,373.1 us) | 5.4% |
| partitions | 150 | 1 | 1 | 1 | 1 | 0.0% |
| wall_ns | 150 | 549,600 (549.6 us) | 903,600 (903.6 us) | 500,300 (500.3 us) | 1,558,300 (1,558.3 us) | 4.5% |

## b.seq.publish_read

| metric | n | P50 | P95 | min | max | inter-run median spread |
| --- | --- | --- | --- | --- | --- | --- |
| batch_count | 75 | 1 | 1 | 1 | 1 | 0.0% |
| canonical_batch_count | 75 | 1 | 1 | 1 | 1 | 0.0% |
| canonical_bytes | 75 | 29,880 | 29,880 | 29,880 | 29,880 | 0.0% |
| canonical_input_bytes | 75 | 36,916 | 36,916 | 36,916 | 36,916 | 0.0% |
| canonical_ns | 75 | 46,400 (46.4 us) | 68,600 (68.6 us) | 33,500 (33.5 us) | 72,900 (72.9 us) | 21.5% |
| conn_open_configure_ns | 75 | 2,237,100 (2,237.1 us) | 2,943,100 (2,943.1 us) | 1,985,200 (1,985.2 us) | 4,826,400 (4,826.4 us) | 15.9% |
| conn_open_count | 75 | 5 | 5 | 5 | 5 | 0.0% |
| conn_open_open_ns | 75 | 142,500 (142.5 us) | 211,200 (211.2 us) | 123,700 (123.7 us) | 404,000 (404.0 us) | 23.2% |
| conn_open_pragma_count | 75 | 15 | 15 | 15 | 15 | 0.0% |
| db_op_load_manifest_commit_ns | 75 | 0 | 0 | 0 | 0 | n/a |
| db_op_load_manifest_count | 75 | 3 | 3 | 3 | 3 | 0.0% |
| db_op_load_manifest_open_ns | 75 | 1,391,100 (1,391.1 us) | 1,905,200 (1,905.2 us) | 1,250,200 (1,250.2 us) | 3,880,900 (3,880.9 us) | 11.3% |
| db_op_load_manifest_opens | 75 | 3 | 3 | 3 | 3 | 0.0% |
| db_op_load_manifest_stmt_ns | 75 | 151,200 (151.2 us) | 237,600 (237.6 us) | 124,500 (124.5 us) | 637,500 (637.5 us) | 17.4% |
| db_op_load_manifest_txn_begin_ns | 75 | 0 | 0 | 0 | 0 | n/a |
| db_op_load_manifest_wall_ns | 75 | 1,541,000 (1,541.0 us) | 2,116,500 (2,116.5 us) | 1,375,000 (1,375.0 us) | 4,519,000 (4,519.0 us) | 12.2% |
| db_op_manifest_commit_commit_ns | 75 | 23,800 (23.8 us) | 63,500 (63.5 us) | 19,300 (19.3 us) | 139,000 (139.0 us) | 43.3% |
| db_op_manifest_commit_count | 75 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_open_ns | 75 | 456,300 (456.3 us) | 703,100 (703.1 us) | 407,000 (407.0 us) | 1,093,300 (1,093.3 us) | 30.6% |
| db_op_manifest_commit_opens | 75 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_manifest_commit_stmt_ns | 75 | 63,600 (63.6 us) | 105,200 (105.2 us) | 54,000 (54.0 us) | 290,300 (290.3 us) | 29.0% |
| db_op_manifest_commit_txn_begin_ns | 75 | 2,800 (2.8 us) | 6,800 (6.8 us) | 2,400 (2.4 us) | 18,200 (18.2 us) | 35.7% |
| db_op_manifest_commit_wall_ns | 75 | 549,000 (549.0 us) | 873,300 (873.3 us) | 498,300 (498.3 us) | 1,347,100 (1,347.1 us) | 29.2% |
| db_op_publication_journal_commit_ns | 75 | 17,000 (17.0 us) | 36,800 (36.8 us) | 13,000 (13.0 us) | 43,300 (43.3 us) | 51.8% |
| db_op_publication_journal_count | 75 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_open_ns | 75 | 461,400 (461.4 us) | 685,500 (685.5 us) | 415,400 (415.4 us) | 900,100 (900.1 us) | 36.5% |
| db_op_publication_journal_opens | 75 | 1 | 1 | 1 | 1 | 0.0% |
| db_op_publication_journal_stmt_ns | 75 | 58,200 (58.2 us) | 97,600 (97.6 us) | 49,300 (49.3 us) | 116,400 (116.4 us) | 29.9% |
| db_op_publication_journal_txn_begin_ns | 75 | 2,900 (2.9 us) | 6,200 (6.2 us) | 2,500 (2.5 us) | 21,500 (21.5 us) | 27.6% |
| db_op_publication_journal_wall_ns | 75 | 545,200 (545.2 us) | 830,000 (830.0 us) | 482,700 (482.7 us) | 1,005,900 (1,005.9 us) | 34.4% |
| install_dir_fsync_count | 75 | 2 | 2 | 2 | 2 | 0.0% |
| install_dir_fsync_ns | 75 | 3,100 (3.1 us) | 4,700 (4.7 us) | 2,800 (2.8 us) | 7,700 (7.7 us) | 22.6% |
| install_rename_count | 75 | 1 | 1 | 1 | 1 | 0.0% |
| install_rename_ns | 75 | 6,800 (6.8 us) | 10,300 (10.3 us) | 4,900 (4.9 us) | 35,600 (35.6 us) | 33.8% |
| install_wall_ns | 75 | 12,200 (12.2 us) | 19,000 (19.0 us) | 10,300 (10.3 us) | 47,900 (47.9 us) | 28.1% |
| parquet_bytes_written | 75 | 19,031 | 19,031 | 19,031 | 19,031 | 0.0% |
| parquet_create_ns | 75 | 3,700 (3.7 us) | 5,400 (5.4 us) | 2,700 (2.7 us) | 17,600 (17.6 us) | 24.3% |
| parquet_digest_ns | 75 | 13,700 (13.7 us) | 18,600 (18.6 us) | 13,000 (13.0 us) | 40,600 (40.6 us) | 10.1% |
| parquet_encode_ns | 75 | 237,400 (237.4 us) | 342,800 (342.8 us) | 197,300 (197.3 us) | 574,700 (574.7 us) | 29.0% |
| parquet_fsync_ns | 75 | 400 | 700 | 200 | 1,300 (1.3 us) | 0.0% |
| parquet_reread_bytes | 75 | 19,031 | 19,031 | 19,031 | 19,031 | 0.0% |
| parquet_reread_passes | 75 | 1 | 1 | 1 | 1 | 0.0% |
| parquet_rewind_ns | 75 | 400 | 900 | 300 | 1,100 (1.1 us) | 25.0% |
| parquet_stat_ns | 75 | 800 | 1,800 (1.8 us) | 600 | 3,000 (3.0 us) | 37.5% |
| parquet_total_ns | 75 | 256,700 (256.7 us) | 371,600 (371.6 us) | 215,400 (215.4 us) | 619,500 (619.5 us) | 28.5% |
| parquet_write_count | 75 | 1 | 1 | 1 | 1 | 0.0% |
| partition_install_count | 75 | 1 | 1 | 1 | 1 | 0.0% |
| reloaded_partitions | 75 | 1 | 1 | 1 | 1 | 0.0% |
| verify_reread_bytes | 75 | 38,062 | 38,062 | 38,062 | 38,062 | 0.0% |
| verify_reread_count | 75 | 2 | 2 | 2 | 2 | 0.0% |
| verify_reread_ns | 75 | 28,900 (28.9 us) | 45,200 (45.2 us) | 25,900 (25.9 us) | 93,900 (93.9 us) | 13.2% |
| verify_reread_passes | 75 | 2 | 2 | 2 | 2 | 0.0% |
| wall_ns | 75 | 3,849,200 (3,849.2 us) | 5,121,300 (5,121.3 us) | 3,434,300 (3,434.3 us) | 9,587,900 (9,587.9 us) | 20.0% |

## concurrency scenario summaries (one per run)

- {"kind": "info", "publisher_busy": 0, "publisher_errors": 0, "publisher_op_count": 202, "publisher_p50_ns": 14152000, "publisher_p95_ns": 19554299, "publisher_threads": 2, "reader_busy": 0, "reader_errors": 0, "reader_op_count": 866, "reader_p50_ns": 6662200, "reader_p95_ns": 9377600, "reader_threads": 4, "sample_note": "per-thread samples capped at 10,000", "scenario": "b.conc.mixed", "sqlite_fd_peak_sampled": 13, "total_fd_peak_sampled": 19}
- {"kind": "info", "publisher_busy": 0, "publisher_errors": 0, "publisher_op_count": 226, "publisher_p50_ns": 13222700, "publisher_p95_ns": 15895600, "publisher_threads": 2, "reader_busy": 0, "reader_errors": 0, "reader_op_count": 978, "reader_p50_ns": 6107400, "reader_p95_ns": 7486400, "reader_threads": 4, "sample_note": "per-thread samples capped at 10,000", "scenario": "b.conc.mixed", "sqlite_fd_peak_sampled": 13, "total_fd_peak_sampled": 19}
- {"kind": "info", "publisher_busy": 0, "publisher_errors": 0, "publisher_op_count": 207, "publisher_p50_ns": 13985900, "publisher_p95_ns": 17938700, "publisher_threads": 2, "reader_busy": 0, "reader_errors": 0, "reader_op_count": 924, "reader_p50_ns": 6412899, "reader_p95_ns": 8570000, "reader_threads": 4, "sample_note": "per-thread samples capped at 10,000", "scenario": "b.conc.mixed", "sqlite_fd_peak_sampled": 13, "total_fd_peak_sampled": 19}
- {"kind": "info", "publisher_busy": 0, "publisher_errors": 0, "publisher_op_count": 193, "publisher_p50_ns": 15025700, "publisher_p95_ns": 20674300, "publisher_threads": 2, "reader_busy": 0, "reader_errors": 0, "reader_op_count": 852, "reader_p50_ns": 6813300, "reader_p95_ns": 10217900, "reader_threads": 4, "sample_note": "per-thread samples capped at 10,000", "scenario": "b.conc.mixed", "sqlite_fd_peak_sampled": 13, "total_fd_peak_sampled": 19}
- {"kind": "info", "publisher_busy": 0, "publisher_errors": 0, "publisher_op_count": 205, "publisher_p50_ns": 14475000, "publisher_p95_ns": 17795799, "publisher_threads": 2, "reader_busy": 0, "reader_errors": 0, "reader_op_count": 909, "reader_p50_ns": 6540700, "reader_p95_ns": 8068800, "reader_threads": 4, "sample_note": "per-thread samples capped at 10,000", "scenario": "b.conc.mixed", "sqlite_fd_peak_sampled": 13, "total_fd_peak_sampled": 19}

## witnesses (one block per run)

- {"digest_match": true, "file_sha256": "635e8bc6b164d48c594ffdcb39b4eee4ea25231288e3f5418486a50bdea1af45", "file_size": 19031, "kind": "witness", "manifest_digest": "635e8bc6b164d48c594ffdcb39b4eee4ea25231288e3f5418486a50bdea1af45", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "5313bb49-2816-4ec4-a1bf-ea6fef2a952c", "stored_byte_count": 19031, "verify_ok": true, "version_digest": "58acb75ed8b846cd91dea1e67103cf46c701077e9fb485a98001d4bffd81f09d"}
- {"digest_match": true, "file_sha256": "d4798d6d2255d35a881db836eb8c3d8d93610022b603d59364b1ca82d684c161", "file_size": 2781937, "kind": "witness", "manifest_digest": "d4798d6d2255d35a881db836eb8c3d8d93610022b603d59364b1ca82d684c161", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "de2b2b0d-9907-4f90-91bf-90db36f1690e", "stored_byte_count": 2781937, "verify_ok": true, "version_digest": "cbe90e54c8ea2d2b3bf647648a87c78a64fe173a330d70e236f33938910d35d6"}
- {"digest_match": true, "file_sha256": "15be6df51b5b0d51dfa7add08da8639ed3380208d7a5c9010ecf948e2c2ecb42", "file_size": 1695288, "kind": "witness", "manifest_digest": "15be6df51b5b0d51dfa7add08da8639ed3380208d7a5c9010ecf948e2c2ecb42", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "3f9f83ed-33cc-4e43-b43f-ca431e4f9258", "stored_byte_count": 1695288, "verify_ok": true, "version_digest": "0dd4b83ed0768b734cf6d66fb8fa8bacc2fb0843035857c2c6184898bc40aed1"}
- {"digest_match": true, "file_sha256": "1410708cdf7f69210b8a4e368912e2c1c06e90fba116208b2f4a9040aa584925", "file_size": 2075930, "kind": "witness", "manifest_digest": "1410708cdf7f69210b8a4e368912e2c1c06e90fba116208b2f4a9040aa584925", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "445424af-7699-4e4d-9eb9-1c72bcc18e01", "stored_byte_count": 2075930, "verify_ok": true, "version_digest": "970d6be7510427c5db4b0a2076ac92b82c3e4cfc55b20b38827ce452fca8e78c"}
- {"kind": "witness", "manifest_not_found": true, "recorded_manifest_commit": false, "recorded_parquet_write": true, "recorded_partition_install": false, "recover_examined": 0, "recover_recovered": 0, "scenario": "a.fail.drop", "snapshot_id": "a7b78fcb-dd70-4e98-96e3-6afec37854a9", "staged_present_after_append": true, "staging_removed_after_drop": true}
- {"corrupted_offset": 9515, "corruption_detected": true, "kind": "witness", "read_path_error": "snapshot 65afbd02-72dd-45d6-a0e1-7b3eb35fddf8 partition 0 failed integrity verification: DigestMismatch", "scenario": "a.fail.corrupt", "snapshot_id": "65afbd02-72dd-45d6-a0e1-7b3eb35fddf8", "verify_error": "snapshot 65afbd02-72dd-45d6-a0e1-7b3eb35fddf8 partition 0 failed integrity verification: DigestMismatch", "verify_reread_events_recorded": true}
- {"digest_match": true, "file_sha256": "9693f6b417adc19e0bcf93faead86fa7c15d7b981d7c63b624faec96cef28335", "file_size": 19031, "kind": "witness", "manifest_digest": "9693f6b417adc19e0bcf93faead86fa7c15d7b981d7c63b624faec96cef28335", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "baf0e1b9-2b68-49f0-936f-10f66350cbe2", "stored_byte_count": 19031, "verify_ok": true, "version_digest": "2410e3d682cae4bf424ccfaf0503aaf6fd5b78c0a3c6f5f29bd762b6e88bc39a"}
- {"digest_match": true, "file_sha256": "b051e5a8d6239f90f39d5f1a7a68c6fe91e795df1dab23346626141e884f559f", "file_size": 2781937, "kind": "witness", "manifest_digest": "b051e5a8d6239f90f39d5f1a7a68c6fe91e795df1dab23346626141e884f559f", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "4fd9d704-729a-4515-9313-d7e951a800fa", "stored_byte_count": 2781937, "verify_ok": true, "version_digest": "bc56ddcc245424ddb18fbe5a5f74cd11fdc33d836b5272a9924cf63c7e7a4959"}
- {"digest_match": true, "file_sha256": "ef1c98c687b513348225e702fb2fc641a30287154f99581cab23b8e1648059dc", "file_size": 1695288, "kind": "witness", "manifest_digest": "ef1c98c687b513348225e702fb2fc641a30287154f99581cab23b8e1648059dc", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "571d63de-24ab-4efb-b5d7-766d3e595fce", "stored_byte_count": 1695288, "verify_ok": true, "version_digest": "be1059953340c5a52dbc60799e6e1a65f5bfd52b8aaa5594314d2fe414b7b9a0"}
- {"digest_match": true, "file_sha256": "c0af914a3cd5ae68b87c5b8f0835a26b299ab877c8ea7ec59bb7c9c70b2fd982", "file_size": 2075930, "kind": "witness", "manifest_digest": "c0af914a3cd5ae68b87c5b8f0835a26b299ab877c8ea7ec59bb7c9c70b2fd982", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "c4e4e5d3-f05f-4d92-a7ff-2ab19ea6670e", "stored_byte_count": 2075930, "verify_ok": true, "version_digest": "71100615305b7e918dbc6cc441d389e5ad44fedd3bb275b33375072481057e6d"}
- {"kind": "witness", "manifest_not_found": true, "recorded_manifest_commit": false, "recorded_parquet_write": true, "recorded_partition_install": false, "recover_examined": 0, "recover_recovered": 0, "scenario": "a.fail.drop", "snapshot_id": "6e13befe-2475-4711-9fa9-ce9e3e8160b4", "staged_present_after_append": true, "staging_removed_after_drop": true}
- {"corrupted_offset": 9515, "corruption_detected": true, "kind": "witness", "read_path_error": "snapshot 98922f0b-16e0-437a-8f82-3459a1938edd partition 0 failed integrity verification: DigestMismatch", "scenario": "a.fail.corrupt", "snapshot_id": "98922f0b-16e0-437a-8f82-3459a1938edd", "verify_error": "snapshot 98922f0b-16e0-437a-8f82-3459a1938edd partition 0 failed integrity verification: DigestMismatch", "verify_reread_events_recorded": true}
- {"digest_match": true, "file_sha256": "625e83e96cbf707da02441038956e9b897a13f6fc3d636008682a514f9fc7025", "file_size": 19031, "kind": "witness", "manifest_digest": "625e83e96cbf707da02441038956e9b897a13f6fc3d636008682a514f9fc7025", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "f297142e-ee5d-4634-be48-eab183a9adfc", "stored_byte_count": 19031, "verify_ok": true, "version_digest": "c11ea23dbd96865cdfce93aeb8a1eacb91721304cedeab41d3e0f843d18c8945"}
- {"digest_match": true, "file_sha256": "c1cb1cfbf0b7f95ca788ff36b0b2fe5286b84b640ec8e4c7656399281dd69ded", "file_size": 2781937, "kind": "witness", "manifest_digest": "c1cb1cfbf0b7f95ca788ff36b0b2fe5286b84b640ec8e4c7656399281dd69ded", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "0a157d5e-b90b-4b59-af4a-4ed812aac5e4", "stored_byte_count": 2781937, "verify_ok": true, "version_digest": "badbfd0cea70fb288440eb9aacca0c32aff38ffbd11987cacb153de642435853"}
- {"digest_match": true, "file_sha256": "95f9ed76d0375411b9f3560304719bf2e7749444c721c4c4bfcf200e665c667e", "file_size": 1695288, "kind": "witness", "manifest_digest": "95f9ed76d0375411b9f3560304719bf2e7749444c721c4c4bfcf200e665c667e", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "7d4daf38-ce7a-480c-947c-1ee8977d6a07", "stored_byte_count": 1695288, "verify_ok": true, "version_digest": "6cdeca02715ef85f919278242eaa37e4d07fa5aa0519ed4e28b2abd4644a8873"}
- {"digest_match": true, "file_sha256": "7cf3b8b06c29e025bafa27e31991bf193afe9371e8d7e8aff41d7c64677c87a9", "file_size": 2075930, "kind": "witness", "manifest_digest": "7cf3b8b06c29e025bafa27e31991bf193afe9371e8d7e8aff41d7c64677c87a9", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "eb57dc80-0934-4e2e-a563-e0f910d2ac32", "stored_byte_count": 2075930, "verify_ok": true, "version_digest": "0e144f24bd04b64a5c305dc165844bc0637ff2f6bbeb9c80aaae007240ce88d0"}
- {"kind": "witness", "manifest_not_found": true, "recorded_manifest_commit": false, "recorded_parquet_write": true, "recorded_partition_install": false, "recover_examined": 0, "recover_recovered": 0, "scenario": "a.fail.drop", "snapshot_id": "ef6cca42-6bc4-4a3d-9e18-4fb00caaa92c", "staged_present_after_append": true, "staging_removed_after_drop": true}
- {"corrupted_offset": 9515, "corruption_detected": true, "kind": "witness", "read_path_error": "snapshot 0265a7e8-802e-4bf6-bca3-b52d97213d5a partition 0 failed integrity verification: DigestMismatch", "scenario": "a.fail.corrupt", "snapshot_id": "0265a7e8-802e-4bf6-bca3-b52d97213d5a", "verify_error": "snapshot 0265a7e8-802e-4bf6-bca3-b52d97213d5a partition 0 failed integrity verification: DigestMismatch", "verify_reread_events_recorded": true}
- {"digest_match": true, "file_sha256": "a64cb2e1bcf5893881620afa0dd08e86f41bf679d3a15d7dae3c1e939577ef5c", "file_size": 19031, "kind": "witness", "manifest_digest": "a64cb2e1bcf5893881620afa0dd08e86f41bf679d3a15d7dae3c1e939577ef5c", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "95937ac6-dedf-4d65-8e10-b16059218024", "stored_byte_count": 19031, "verify_ok": true, "version_digest": "a86c4e8ffafc810db3095763b1828011bb704682ba426f7a5eeebbbbb61b1a49"}
- {"digest_match": true, "file_sha256": "33fab0f3631115c59742e6c26b5aa8cb39273f1a4a25e4a16cd759f83b0f9add", "file_size": 2781937, "kind": "witness", "manifest_digest": "33fab0f3631115c59742e6c26b5aa8cb39273f1a4a25e4a16cd759f83b0f9add", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "f45f45a1-2d98-4a74-a833-10f4b611ad19", "stored_byte_count": 2781937, "verify_ok": true, "version_digest": "ea2093ccd0d6b93c6c031eb4bbae156fd4722ea7d3e68a25c693f121c65445b9"}
- {"digest_match": true, "file_sha256": "1d798b857cc71615f34068f5da71a9a67580e1a3834a6e3e4392480106f5ba2c", "file_size": 1695288, "kind": "witness", "manifest_digest": "1d798b857cc71615f34068f5da71a9a67580e1a3834a6e3e4392480106f5ba2c", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "b2c66b47-77e2-4ec9-aee0-fefb0bc6f156", "stored_byte_count": 1695288, "verify_ok": true, "version_digest": "573f6c70ae76c6ad24e083817865735c315d6c33d1ab9e3703d4063452a1f9ba"}
- {"digest_match": true, "file_sha256": "af87345adc7b2ecd1ccde70393cdf9a30a4425b523e439387b3981f0878a4c6c", "file_size": 2075930, "kind": "witness", "manifest_digest": "af87345adc7b2ecd1ccde70393cdf9a30a4425b523e439387b3981f0878a4c6c", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "d716c833-6254-4f42-b815-85124f47bd6c", "stored_byte_count": 2075930, "verify_ok": true, "version_digest": "9ff1affdca16dca00bbff022b65b5ea2ce35c694416b595617ca0e615cf70261"}
- {"kind": "witness", "manifest_not_found": true, "recorded_manifest_commit": false, "recorded_parquet_write": true, "recorded_partition_install": false, "recover_examined": 0, "recover_recovered": 0, "scenario": "a.fail.drop", "snapshot_id": "4fddde4b-f6ab-492f-8ced-270617e7da79", "staged_present_after_append": true, "staging_removed_after_drop": true}
- {"corrupted_offset": 9515, "corruption_detected": true, "kind": "witness", "read_path_error": "snapshot de501b50-b1d1-4fb1-a2b4-75ed64974674 partition 0 failed integrity verification: DigestMismatch", "scenario": "a.fail.corrupt", "snapshot_id": "de501b50-b1d1-4fb1-a2b4-75ed64974674", "verify_error": "snapshot de501b50-b1d1-4fb1-a2b4-75ed64974674 partition 0 failed integrity verification: DigestMismatch", "verify_reread_events_recorded": true}
- {"digest_match": true, "file_sha256": "b05cf5f969d17d1456caacd27a44f762b678e7348266448038ee10cf9f3e9aab", "file_size": 19031, "kind": "witness", "manifest_digest": "b05cf5f969d17d1456caacd27a44f762b678e7348266448038ee10cf9f3e9aab", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "e4415be6-e498-46e4-9be5-e0ae624fe525", "stored_byte_count": 19031, "verify_ok": true, "version_digest": "f4f9a2afc0efe3df046e611ffd181e1a9b192e0d71a5dec6ebf0b58abe235663"}
- {"digest_match": true, "file_sha256": "b77114bab7482094dbfa2a1f2cc577ec5e0dd2180ef2d3b34570531832153ed3", "file_size": 2781937, "kind": "witness", "manifest_digest": "b77114bab7482094dbfa2a1f2cc577ec5e0dd2180ef2d3b34570531832153ed3", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "a62c5775-d9b0-44d2-92f7-e88c5d11375c", "stored_byte_count": 2781937, "verify_ok": true, "version_digest": "cebe5b851c000930beba9983f063d1cbd272924c3f76d7c0f0059375bc3eea31"}
- {"digest_match": true, "file_sha256": "bdf4cf0dc93a2eec956e63c0c1b22460319d0c132b033b808f427a84f98cd124", "file_size": 1695288, "kind": "witness", "manifest_digest": "bdf4cf0dc93a2eec956e63c0c1b22460319d0c132b033b808f427a84f98cd124", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "249f564e-7700-40dd-b709-0b4f7d5bf607", "stored_byte_count": 1695288, "verify_ok": true, "version_digest": "d02826aed1da82b655d1f84c44ce1913389221872aa11d3b083e456e2a655c30"}
- {"digest_match": true, "file_sha256": "7b05e2cbf69f184ed016f5409a7b86b7875d7ac91c7d7c381f76bcc562145f28", "file_size": 2075930, "kind": "witness", "manifest_digest": "7b05e2cbf69f184ed016f5409a7b86b7875d7ac91c7d7c381f76bcc562145f28", "partition_count": 1, "scenario": "write_witness", "size_match": true, "snapshot_id": "e73dfab7-f0a8-4010-a80d-51d6c03ccc02", "stored_byte_count": 2075930, "verify_ok": true, "version_digest": "80c2fe346a8b87163c582456434228605b285f48a1ff287f65a63825a33b94a4"}
- {"kind": "witness", "manifest_not_found": true, "recorded_manifest_commit": false, "recorded_parquet_write": true, "recorded_partition_install": false, "recover_examined": 0, "recover_recovered": 0, "scenario": "a.fail.drop", "snapshot_id": "1a3c89b1-00ec-4b06-86ac-58c0ec69a6be", "staged_present_after_append": true, "staging_removed_after_drop": true}
- {"corrupted_offset": 9515, "corruption_detected": true, "kind": "witness", "read_path_error": "snapshot 53635bb3-173a-452e-8c70-b8b983cd3bbd partition 0 failed integrity verification: DigestMismatch", "scenario": "a.fail.corrupt", "snapshot_id": "53635bb3-173a-452e-8c70-b8b983cd3bbd", "verify_error": "snapshot 53635bb3-173a-452e-8c70-b8b983cd3bbd partition 0 failed integrity verification: DigestMismatch", "verify_reread_events_recorded": true}

