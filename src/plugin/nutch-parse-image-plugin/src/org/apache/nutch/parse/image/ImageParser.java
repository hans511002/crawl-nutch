package org.apache.nutch.parse.image;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageParser implements Parser
{
	public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.image");
	private Configuration conf = null;

	public ImageParser()
	{
	}

	public void setConf(Configuration conf)
	{
		this.conf = conf;
	}

	public Configuration getConf()
	{
		return conf;
	}

	@SuppressWarnings("unchecked")
	public ParseResult getParse(Content content)
	{
		String text = "";
		@SuppressWarnings("rawtypes")
		Vector outlinks = new Vector();

		try
		{

			byte[] raw = content.getContent();
			String contentLength = content.getMetadata().get(
					Response.CONTENT_LENGTH);
			if (contentLength != null
					&& raw.length != Integer.parseInt(contentLength))
			{
				return new ParseStatus(
						ParseStatus.FAILED,
						ParseStatus.FAILED_TRUNCATED,
						"Content truncated at "
								+ raw.length
								+ " bytes. Parser can't handle incomplete files.")
						.getEmptyParseResult(content.getUrl(), getConf());
			}
		}
		catch (Exception e)
		{ // run time exception
			LOG.error("Error, runtime exception: ", e);
			return new ParseStatus(ParseStatus.FAILED,
					"Can't be handled as SWF document. " + e)
					.getEmptyParseResult(content.getUrl(), getConf());
		}

		Outlink[] links = (Outlink[]) outlinks.toArray(new Outlink[outlinks
				.size()]);
		ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "",
				links, content.getMetadata());
		return ParseResult.createParseResult(content.getUrl(), new ParseImpl(
				text, parseData));
	}

	/**
	 * Arguments are: 0. Name of input jpg file.
	 */
	public static void main(String[] args) throws IOException
	{
		FileInputStream in = new FileInputStream(args[0]);

		byte[] buf = new byte[in.available()];
		in.read(buf);
		ImageParser parser = new ImageParser();
		ParseResult parseResult = parser.getParse(new Content(
				"file:" + args[0], "file:" + args[0], buf,
				"image/jpeg", new Metadata(),
				NutchConfiguration.create()));
		Parse p = parseResult.get("file:" + args[0]);
		System.out.println("Parse Text:");
		System.out.println(p.getText());
		System.out.println("Parse Data:");
		System.out.println(p.getData());
	}
}