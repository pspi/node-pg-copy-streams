const EventEmitter = require("events");

const protocolMessageTypeByte = 1;
const protocolContentLengthBytes = 4;

class ProtocolReader extends EventEmitter {
  constructor() {
    super();
    this.unreadContentBytes = 0;
    this.chunk = Buffer.from([]);
  }

  read(incomingChunk) {
    this._mergeIncomingChunk(incomingChunk);
    while (this.chunk.length > 0) {
      if (this.unreadContentBytes > 0) {
        this._consumeContent();
      } else if (this.chunk.length >= protocolMessageTypeByte + protocolContentLengthBytes) {
        this._consumeMessageTypeAndContentLength();
      } else {
        break;
      }
    }
  }

  _mergeIncomingChunk(incomingChunk) {
    if (this.chunk.length > 0) {
      this.chunk = Buffer.concat([this.chunk, incomingChunk]);
    } else {
      this.chunk = incomingChunk;
    }
  }

  _consumeContent() {
    const theoreticalContentLastIndexExclusive = this.unreadContentBytes;
    const chunkLastIndexExclusive = this.chunk.length;
    const endIndexExclusive = Math.min(theoreticalContentLastIndexExclusive, chunkLastIndexExclusive);

    this.emit("content", this.chunk.slice(0, endIndexExclusive));

    const bytesConsumed = endIndexExclusive;
    this.unreadContentBytes -= bytesConsumed;

    this.chunk = this.chunk.slice(bytesConsumed);
  }

  _consumeMessageTypeAndContentLength() {
    const messageType = this.chunk[0];
    const contentLength = this.chunk.readUInt32BE(protocolMessageTypeByte);
    const netContentLength = contentLength - protocolContentLengthBytes;

    this.emit("message", messageType);

    const bytesConsumed = protocolMessageTypeByte + protocolContentLengthBytes;
    this.unreadContentBytes = netContentLength;

    this.chunk = this.chunk.slice(bytesConsumed);
  }
}

module.exports = {
  ProtocolReader,
};
