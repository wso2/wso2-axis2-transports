/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.tcp;

import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisEngine;
import org.apache.axis2.util.MessageContextBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.net.Socket;
import java.util.Arrays;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

/**
 * This Class is the work hoarse of the TCP request, this process the incomming SOAP Message.
 */
public class TCPWorker implements Runnable {

	private static final Log log = LogFactory.getLog(TCPWorker.class);

	private TCPEndpoint endpoint;
	private Socket socket;

	public TCPWorker(TCPEndpoint endpoint, Socket socket) {
		this.endpoint = endpoint;
		this.socket = socket;
	}

	public void run() {
		MessageContext msgContext = null;
		try {
			msgContext = endpoint.createMessageContext();
			msgContext.setIncomingTransportName(Constants.TRANSPORT_TCP);
			TCPOutTransportInfo outInfo = new TCPOutTransportInfo();
			outInfo.setSocket(socket);
			outInfo.setClientResponseRequired(endpoint.isClientResponseRequired());
			outInfo.setContentType(endpoint.getContentType());
			String delimiter = endpoint.getRecordDelimiter();
			int recordLength = endpoint.getRecordLength();
			String inputType = endpoint.getInputType();
			String delimiterType = endpoint.getRecordDelimiterType();
			outInfo.setDelimiter(delimiter);
			outInfo.setDelimiterType(delimiterType);
			msgContext.setProperty(Constants.OUT_TRANSPORT_INFO, outInfo);
			// create the SOAP Envelope
			InputStream input = socket.getInputStream();
			boolean handled = false;
			if  (recordLength > -1) {
				this.handleRecordLength(msgContext, input, recordLength);
				handled = true;
			}

			int delimiterLength = endpoint.getRecordDelimiterLength();
			if (delimiterLength > -1) {
				this.handleRecordDelimiterLengthStream(msgContext, input, delimiterLength, delimiterType);
				handled = true;
			}

			if (!delimiter.isEmpty() && !handled) {
				if (inputType != null) {
					if (TCPConstants.STRING_INPUT_TYPE.equalsIgnoreCase(inputType)) {
						if(TCPConstants.BYTE_DELIMITER_TYPE.equalsIgnoreCase(delimiterType)) {
							int delimiterVal = Integer.parseInt(delimiter.split("0x")[1], 16);
							this.handleCharacterRecordDelimiterStringStream(msgContext, input, delimiterVal);
						} else if(TCPConstants.STRING_DELIMITER_TYPE.equalsIgnoreCase(delimiterType)) {
							this.handleStringRecordDelimiterStringStream(msgContext, input, delimiter);
						} else {
							this.handleCharacterRecordDelimiterStringStream(msgContext, input, delimiter.charAt(0));
						}
					} else {
						if(TCPConstants.BYTE_DELIMITER_TYPE.equalsIgnoreCase(delimiterType)) {
							int delimiterVal = Integer.parseInt(delimiter.split("0x")[1], 16);
							this.handleCharacterRecordDelimiterBinaryStream(msgContext, input, delimiterVal);
						} else if(TCPConstants.STRING_DELIMITER_TYPE.equalsIgnoreCase(delimiterType)) {
							this.handleStringRecordDelimiterBinaryStream(msgContext, input, delimiter);
						} else {
							this.handleCharacterRecordDelimiterBinaryStream(msgContext, input, delimiter.charAt(0));
						}
					}
				}
				handled = true;
			}

			if (!handled) {
				SOAPEnvelope envelope = TransportUtils.createSOAPMessage(msgContext, input,
						endpoint.getContentType());
				msgContext.setEnvelope(envelope);
				AxisEngine.receive(msgContext);
			}
		} catch (Exception e) {
			sendFault(msgContext, e);
		} finally {
			if (!endpoint.isClientResponseRequired()) {
				try {
					socket.close();
				} catch (IOException e) {
					log.error("Error while closing a TCP socket", e);
				}
			}
		}
	}

	private void handleRecordDelimiterLengthStream(MessageContext msgContext, InputStream input, int delimiterLength, String delimiterType) throws AxisFault {
		try {
			int		delimiterLengthCount	= 0;
			int		recordLengthCount		= 0;
			int		recordLength			= 0;
			byte[]	recordBytes				= null;
			byte[]	delimiterBytes			= new byte[delimiterLength];
			boolean isBinaryDelimiter		= delimiterType.equalsIgnoreCase("binary") ? true : false;

			int readCount;
			while (true) {
				while (delimiterLengthCount < delimiterLength) {
					readCount = input.read(delimiterBytes, delimiterLengthCount, delimiterLength - delimiterLengthCount);
					if (readCount == -1) {
						return;
					}
					delimiterLengthCount += readCount;
				}

				if (isBinaryDelimiter) {
					recordLength = convertBinaryBytesToInt(delimiterBytes);
				} else {
					recordLength = convertAsciiBytesToInt(delimiterBytes);
				}

				recordBytes = new byte[recordLength];
				while (recordLengthCount < recordLength) {
					readCount = input.read(recordBytes, recordLengthCount, recordLength - recordLengthCount);
					if (readCount == -1) {
						return;
					}
					recordLengthCount += readCount;
				}

				handleEnvelope(msgContext, recordBytes);

				delimiterLengthCount 	= 0;
				recordLengthCount		= 0;
				recordBytes				= null;
			}
		} catch (IOException e) {
			sendFault(msgContext, e);
		} finally {
		}
	}

	/**
	 * Handling record delimiter character type for string stream
	 *
	 * @param msgContext the messahe contenxt
	 * @param input socket input stream
	 * @param delimiter character value to delimit incoming message
	 * @throws XMLStreamException if xml parsing error occurred
	 * @throws FactoryConfigurationError if configuration issue occurred
	 */
	private void handleCharacterRecordDelimiterStringStream(MessageContext msgContext, InputStream input,
															int delimiter) throws AxisFault {
		if(log.isDebugEnabled()) {
			log.debug("Handle message with character delimiter type string stream");
		}
		StreamTokenizer tokenizer = new StreamTokenizer(new InputStreamReader(input));
		tokenizer.resetSyntax();
		tokenizer.wordChars('\u0000', (char) (delimiter - 1));
		tokenizer.wordChars((char) (delimiter + 1), '\u00ff');
		tokenizer.whitespaceChars('\n', '\n');
		tokenizer.whitespaceChars(delimiter, delimiter);
		tokenizer.eolIsSignificant(true);
		int type = 0; // Stores the value returned by nextToken()
		try {
			type = tokenizer.nextToken();
			while (type != StreamTokenizer.TT_EOF) {
				if (type == StreamTokenizer.TT_WORD) {
					handleEnvelope(msgContext, tokenizer.sval.getBytes());
				} else {
					assert false; // We only expect words
				}
				type = tokenizer.nextToken();
			}
		} catch (IOException e) {
			sendFault(msgContext, e);
		}
	}

	/**
	 * Handling record delimiter character type in binary stream
	 *
	 * @param msgContext the message context
	 * @param input socket input stream
	 * @param delimiter character value to delimit incoming message
	 * @throws AxisFault if error occurred while processing
	 */
	private void handleCharacterRecordDelimiterBinaryStream(MessageContext msgContext, InputStream input,
															int delimiter) throws AxisFault {
		if(log.isDebugEnabled()) {
			log.debug("Handle message with character delimiter type binary stream");
		}
		ByteArrayOutputStream bos = null;
		try {
			int next = input.read();
			while (next > -1) {
				if (next == delimiter && bos != null) {
					try {
						bos.flush();
					} catch (IOException e) {
						sendFault(msgContext, e);
					}
					byte[] result = bos.toByteArray();
					handleEnvelope(msgContext, result);
					bos.close();
					bos = null;
					next = input.read();
					continue;
				}

				if (bos == null) {
					bos = new ByteArrayOutputStream();
				}

				if (next != delimiter) {
					bos.write(next);
				}
				next = input.read();
			}
			if (bos != null) {
				handleEnvelope(msgContext, bos.toByteArray());
			}
		} catch (IOException e) {
			sendFault(msgContext, e);
		} finally {
			if (bos != null) {
				try {
					bos.close();
				} catch (IOException e) {
					sendFault(msgContext, e);
				}
			}
		}
	}

	/**
	 * Handling record string type delimiter in string stream
	 *
	 * @param msgContext the message context
	 * @param input socket input stream
	 * @param delimiter delimiter string
	 * @throws AxisFault if error occurred while processing
	 */
	private void handleStringRecordDelimiterStringStream(MessageContext msgContext, InputStream input,
														 String delimiter) throws AxisFault {
		if(log.isDebugEnabled()) {
			log.debug("Handle message with string delimiter type string stream");
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			int next = input.read();
			while(next > -1) {
				bos.write(next);
				next = input.read();
				if(input.available() <= 0) {
					if(next > -1) {
						bos.write(next);
					}
					String[] segments = bos.toString().split(delimiter);
					for(String s : segments) {
						handleEnvelope(msgContext, s.getBytes());
					}
					bos = new ByteArrayOutputStream();
					next = input.read();
				}
			}
		} catch (IOException e) {
			sendFault(msgContext, e);
		} finally {
			try {
				bos.close();
			} catch (IOException e) {
				sendFault(msgContext, e);
			}
		}
	}

	/**
	 * Handling record delimiter string type delimiter in binary stream
	 *
	 * @param msgContext the message context
	 * @param input socket input stream
	 * @param delimiter delimiter string
	 * @throws AxisFault if error occurred while processing
	 */
	private void handleStringRecordDelimiterBinaryStream(MessageContext msgContext, InputStream input,
														 String delimiter) throws AxisFault {
		if(log.isDebugEnabled()) {
			log.debug("Handle message with string delimiter type binary stream");
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ByteArrayOutputStream chunk = new ByteArrayOutputStream();
		try {
			byte[] delimiterBytes = delimiter.getBytes();
			int next = input.read();
			while(next > -1) {
				bos.write(next);
				next = input.read();
				if(input.available() <= 0) {
					if (next > -1) {
						bos.write(next);
					}
					byte[] contents = bos.toByteArray();
					for(int i = 0; i< (contents.length - delimiterBytes.length + 1); i++) {
						byte[] temp = new  byte[delimiterBytes.length];
						int count = 0;
						for(int j = i; j < (i + delimiterBytes.length); j++) {
							temp[count] = contents[j];
							count++;
						}
						if(Arrays.equals(delimiterBytes, temp)) {
							handleEnvelope(msgContext, chunk.toByteArray());
							chunk = new ByteArrayOutputStream();
							i = i + delimiterBytes.length - 1;
						} else {
							chunk.write(contents[i]);
						}
					}
					bos =new ByteArrayOutputStream();
					next = input.read();
				}
			}
		} catch (IOException e) {
			sendFault(msgContext, e);
		} finally {
			try {
				bos.close();
				chunk.close();
			} catch (IOException e) {
				sendFault(msgContext, e);
			}
		}
	}

	/**
	 * Handling record length delimiter
	 * @param msgContext the message context
	 * @param input the socket input stream
	 * @param recordLength the record length to split the message
	 * @throws AxisFault if error occurred while processing
	 */
	private void handleRecordLength(MessageContext msgContext, InputStream input,
									int recordLength) throws AxisFault {
		ByteArrayOutputStream baos = null;
		ByteArrayInputStream bais = null;
		byte[] bytes = new byte[recordLength];
		try {
			for (int len; (len = input.read(bytes)) > 0;) {
				baos = new ByteArrayOutputStream();
				baos.write(bytes, 0, len);
				bais = new ByteArrayInputStream(baos.toByteArray());
				SOAPEnvelope envelope = TransportUtils.createSOAPMessage(msgContext, bais,
						endpoint.getContentType());
				msgContext.setEnvelope(envelope);
				AxisEngine.receive(msgContext);
			}
		} catch (IOException e) {
			sendFault(msgContext, e);
		} catch (XMLStreamException e) {
			sendFault(msgContext, e);
		} finally {
			if(baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
					sendFault(msgContext, e);
				}
				if(bais != null) {
					try {
						bais.close();
					} catch (IOException e) {
						sendFault(msgContext, e);
					}
				}
			}
		}
	}

	private void handleEnvelope(MessageContext msgContext, byte [] value) throws AxisFault {
		ByteArrayInputStream bais = null;
		try {
			bais = new ByteArrayInputStream(value);
			SOAPEnvelope envelope = TransportUtils.createSOAPMessage(msgContext,
					bais, endpoint.getContentType());
			msgContext.setEnvelope(envelope);
			AxisEngine.receive(msgContext);
		} catch (IOException e) {
			sendFault(msgContext, e);
		} catch (XMLStreamException e) {
			sendFault(msgContext, e);
		} finally {
			if(bais != null) {
				try {
					bais.close();
				} catch (IOException e) {
					sendFault(msgContext, e);
				}
			}
		}
	}

	private void sendFault(MessageContext msgContext, Exception fault) {
		log.error("Error while processing TCP request through the Axis2 engine", fault);
		try {
			if (msgContext != null) {
				msgContext.setProperty(MessageContext.TRANSPORT_OUT, socket.getOutputStream());

				MessageContext faultContext =
						MessageContextBuilder.createFaultMessageContext(msgContext, fault);

				AxisEngine.sendFault(faultContext);
			}
		} catch (Exception e) {
			log.error("Error while sending the fault response", e);
		}
	}

	private static int convertBinaryBytesToInt(byte[] input) {
		int     output = 0;
		byte[]  bytes;

		if (input.length > 4) {
			bytes = new byte[4];
			System.arraycopy(input, (input.length - 4), bytes, 0, 4);
		} else {
			bytes = input;
		}

		for (int i = 0; i < bytes.length; i++) {
			output = ((output << 8) & 0xFFFFFF00) + (0x000000FF & bytes[i]);
		}

		return output;
	}

	private static int convertAsciiBytesToInt(byte[] input) {
		int     output = 0;
		int[]   factor  = { 1000, 100, 10, 1 };
		int     slot    = 4 - input.length;

		for (int i = 0; i < input.length; i++) {
			output += (input[i] & 0x0f) * factor[slot + i];
		}

		return output;
	}
}
