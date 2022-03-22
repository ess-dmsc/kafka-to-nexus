from test_filewriter_static_data import test_static_data_reaches_file
from test_epics_status import test_ep00
from test_repeated_messages_logic import test_repeated_messages
from test_filewriter_links import test_links
from test_filewriter_kafka_meta_data import test_end_message_metadata
from test_same_id_multiple_modules import test_two_different_writer_modules_with_same_flatbuffer_id
from test_filewriter_stop_time import test_start_and_stop_time_are_in_the_past
from test_filewriter_ignores_commands import test_ignores_commands_with_incorrect_job_id
from test_f142_meta_data import test_f142_meta_data
from file_writer_control import WorkerJobPool

def main():
  kafka_host = "localhost:9093"
  worker = WorkerJobPool(
      job_topic_url=f"{kafka_host}/TEST_writer_jobs",
      command_topic_url=f"{kafka_host}/TEST_writer_commands")
  list_of_tests = [
    test_static_data_reaches_file,
    test_ep00,
    test_repeated_messages,
    test_links,
    test_end_message_metadata,
    test_two_different_writer_modules_with_same_flatbuffer_id,
    test_start_and_stop_time_are_in_the_past,
    test_ignores_commands_with_incorrect_job_id,
    test_f142_meta_data,
  ]
  for func in list_of_tests:
    for i in range(10):
      func(worker, kafka_host, f"{func.__name__}_{i+1}.nxs")
      print(f"Done with run {i+1} of func {func.__name__}")
  

if __name__ == '__main__':
    main()
