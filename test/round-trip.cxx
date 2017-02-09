	{
		// TODO
		// Move testing code into async test.
		BrokerOpt opt;
		opt.address = "localhost:9092";
		opt.topic = test_data_topic;
		Producer prod(opt);
		ProducerTopic topic(prod, opt.topic);
		for (auto & sourcename : test_source_names) {
			for (int i1 = 0; i1 < 20; ++i1) {
				flatbuffers::FlatBufferBuilder builder(1024);
				auto srcn = builder.CreateString(sourcename);
				std::vector<double> data;
				data.resize(5);
				for (size_t i2 = 0; i2 < data.size(); ++i2) {
					data[i2] = 10000 + 100 * i1 + i2;
				}
				auto v = builder.CreateVector(data);
				BrightnESS::FlatBufs::f140_general::pvDoubleBuilder b1(builder);
				//				b1.add_ts(102030);
				//				b1.add_src(srcn);
				//				b1.add_v(v);
				auto pv = b1.Finish();
				builder.Finish(pv);
				std::vector<char> msg;
				msg.push_back(0x41);
				msg.push_back(0xf1);
				std::copy(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize(), std::back_inserter(msg));
				if (true) {
					// TODO Send off to Kafka and let Streamer fetch it
					topic.produce(msg.data(), msg.size());
					prod.poll();
				}
				else {
					// Feed directly to the demuxers.
					// Works only if we do not give away the file writer task before.
					fwt->demuxers().at(0).process_message(msg.data(), msg.size());
				}
			}
		}
		//fwt->file_flush();
		prod.poll_outq();
	}

	LOG(3, "Waiting for StreamMaster to stop");
	//stream_master.wait_until_n_packets_recv(10)
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	stream_master.stop();

