package org.commoncrawl.examples.mapreduce;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ServerTypeMap {
	private static final Logger LOG = Logger.getLogger(ServerTypeMap.class);

	protected static enum MAPPERCOUNTER {
		RECORDS_IN, //
		NO_SERVER, //
		EXCEPTIONS //
	}

	protected static class ServerMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			for (ArchiveRecord r : value) {
				// Skip any records that are not JSON
				if (!r.getHeader().getMimetype().equals("application/json")) {
					continue;
				}
				try {
					context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
					// Convenience function that reads the full message into a raw byte array
					byte[] rawData = IOUtils.toByteArray(r, r.available());
					String content = new String(rawData);
					JSONObject json = new JSONObject(content);
					try {
						String warcType = json.getJSONObject("Envelope")
								.getJSONObject("WARC-Header-Metadata")
								.getString("WARC-Type");
						if (!warcType.equals("response")) {
							continue;
						}
						JSONObject httpHeaders = json.getJSONObject("Envelope")
								.getJSONObject("Payload-Metadata")
								.getJSONObject("HTTP-Response-Metadata")
								.getJSONObject("Headers");
						JSONArray httpHeaderNames = httpHeaders.names();
						for (int i = 0, l = httpHeaders.length(); i < l; i++) {
							String headerName = httpHeaderNames.getString(i);
							if (headerName.equalsIgnoreCase("server")) {
								Object headerValue = httpHeaders.get(headerName);
								if (headerValue instanceof JSONArray) {
									for (int j = 0, L = ((JSONArray) headerValue).length(); j < L; j++) {
										outKey.set(((JSONArray) headerValue).getString(j));
										context.write(outKey, outVal);
									}
								} else {
									outKey.set(headerValue.toString());
									context.write(outKey, outVal);
								}
							}
						}
					} catch (JSONException ex) {
						LOG.error("Failed to get HTTP header \"Server\" for " + r.getHeader().getUrl(), ex);
					}
				} catch (Exception ex) {
					LOG.error("Caught Exception", ex);
					context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				}
			}
		}
	}
}
