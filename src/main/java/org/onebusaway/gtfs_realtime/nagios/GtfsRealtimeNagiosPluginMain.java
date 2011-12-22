package org.onebusaway.gtfs_realtime.nagios;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.DateFormat;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

import com.google.protobuf.ExtensionRegistry;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedHeader;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtimeOneBusAway;

public class GtfsRealtimeNagiosPluginMain {

  private static final DateFormat _dateFormat = DateFormat.getDateTimeInstance(
      DateFormat.SHORT, DateFormat.SHORT);

  private enum Status {
    OK, WARNING, CRITICAL
  }

  private static final String ARG_CRITICAL_THRESHOLD = "c";

  private static final String ARG_WARNING_THRESHOLD = "w";

  private static final String ARG_URL = "u";

  private static final String ARG_SOURCE = "s";

  private static final ExtensionRegistry _registry = ExtensionRegistry.newInstance();

  static {
    _registry.add(GtfsRealtimeOneBusAway.delay);
    _registry.add(GtfsRealtimeOneBusAway.source);
  }

  public static void main(String[] args) {

    try {
      GtfsRealtimeNagiosPluginMain m = new GtfsRealtimeNagiosPluginMain();
      Status status = m.run(args);

      switch (status) {
        case OK:
          System.exit(0);
          break;
        case WARNING:
          System.exit(1);
          break;
        case CRITICAL:
          System.exit(2);
          break;
      }
    } catch (Exception ex) {
      ex.printStackTrace(System.out);
      System.exit(3);
    }
  }

  private URL _url;

  private int _criticalThreshold = 2;

  private int _warningThreshold = 10;

  private String _source;

  public Status run(String[] args) throws ParseException, IOException {
    if (args.length == 1 && args[0].equals("-h")) {
      System.out.println("usage: -u url [-c critical_num] [-w warning_num] [-s source]");
      System.exit(-1);
    }

    Options options = new Options();
    buildOptions(options);

    Parser parser = new GnuParser();
    CommandLine cli = parser.parse(options, args);

    _url = new URL(cli.getOptionValue(ARG_URL));
    if (cli.hasOption(ARG_CRITICAL_THRESHOLD)) {
      _criticalThreshold = Integer.parseInt(cli.getOptionValue(ARG_CRITICAL_THRESHOLD));
    }
    if (cli.hasOption(ARG_WARNING_THRESHOLD)) {
      _warningThreshold = Integer.parseInt(cli.getOptionValue(ARG_WARNING_THRESHOLD));
    }
    if (cli.hasOption(ARG_SOURCE)) {
      _source = cli.getOptionValue(ARG_SOURCE);
    }

    return fetch();
  }

  private void buildOptions(Options options) {
    Option option = new Option(ARG_URL, true, "");
    option.setRequired(true);
    options.addOption(option);

    options.addOption(ARG_CRITICAL_THRESHOLD, true, "");
    options.addOption(ARG_WARNING_THRESHOLD, true, "");

    options.addOption(ARG_SOURCE, true, "");
  }

  public Status fetch() throws IOException {
    InputStream in = _url.openStream();
    FeedMessage message = FeedMessage.parseFrom(in, _registry);
    int count = countEntities(message);
    Status status = getStatusForCount(count);
    StringBuilder b = new StringBuilder();
    b.append("GTFS-REALTIME ");
    b.append(status);
    b.append(" - count=");
    b.append(count);
    if (message.hasHeader()) {
      FeedHeader header = message.getHeader();
      if (header.hasTimestamp()) {
        Date timestamp = new Date(header.getTimestamp());
        b.append(" time=");
        b.append(_dateFormat.format(timestamp));
      }
    }
    System.out.println(b.toString());
    return status;
  }

  private int countEntities(FeedMessage message) {
    int count = 0;
    for (int i = 0; i < message.getEntityCount(); ++i) {
      FeedEntity entity = message.getEntity(i);
      if (!isSourceMatched(entity)) {
        continue;
      }
      count++;
    }
    return count;
  }

  private boolean isSourceMatched(FeedEntity entity) {
    if (_source == null)
      return true;

    if (!entity.hasExtension(GtfsRealtimeOneBusAway.source)) {
      return false;
    }
    String source = entity.getExtension(GtfsRealtimeOneBusAway.source);
    return _source.equals(source);
  }

  private Status getStatusForCount(int count) {
    if (count <= _criticalThreshold) {
      return Status.CRITICAL;
    } else if (count <= _warningThreshold) {
      return Status.WARNING;
    } else {
      return Status.OK;
    }
  }
}
