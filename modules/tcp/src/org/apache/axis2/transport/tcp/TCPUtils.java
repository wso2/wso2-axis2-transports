/*
 *  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.axis2.transport.tcp;

public class TCPUtils {

    private TCPUtils() {

    }

    public static byte[] convertIntToBinaryBytes(int input, int byteSize) {
        if (byteSize > 4) {
            byteSize = 4;
        }
        byte[] output = new byte[byteSize];
        int[] factor = {0xFF000000, 0x00FF0000, 0x0000FF00, 0x000000FF};
        int slot = 4 - byteSize;
        int mult = byteSize;
        for (int i = 0; i < byteSize; i++) {
            output[i] = (byte) ((input & factor[slot + i]) >> (8 * (--mult)));
        }
        return output;
    }

    public static int convertBinaryBytesToInt(byte[] input) {
        int output = 0;
        byte[] bytes;
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

    public static byte[] convertIntToAsciiBytes(int input, int byteSize) {
        if (byteSize > 4) {
            byteSize = 4;
        }
        byte[] output = new byte[byteSize];
        int[] factor = {1000, 100, 10, 1};
        int slot = 4 - byteSize;
        int mod = input;
        for (int i = 0; i < byteSize; i++) {
            output[i] = (byte) ((mod / factor[slot + i] & 0x0f) | 0x30);
            mod = mod % factor[slot + i];
        }
        return output;
    }

    public static int convertAsciiBytesToInt(byte[] input) {
        int output = 0;
        int[] factor = {1000, 100, 10, 1};
        int slot = 4 - input.length;
        for (int i = 0; i < input.length; i++) {
            output += (input[i] & 0x0f) * factor[slot + i];
        }
        return output;
    }
}
