const Transform = require("stream").Transform;
const util = require("util");

const code = require("./message-formats");
const { ProtocolReader } = require("./protocolreader");

class CopyStreamQuery extends Transform {
  constructor(text, options) {
    super(options);
    this.text = text;
    this.rowCount = 0;

    this.currentMessage;
    this.contentChunks = [];
    this.metCopyDone = false;

    this.protocolReader = new ProtocolReader();
    this.protocolReader.on("message", (code) => this.handleMessage(code));
    this.protocolReader.on("content", (chunk) => this.handleContent(chunk));
  }

  submit(connection) {
    connection.query(this.text);
    connection.removeAllListeners("copyData");
    connection.stream.pipe(this);

    this.connection = connection;
  }

  _detach() {
    this.connection.stream.unpipe(this);

    // unpipe can pause the stream but also underlying onData event can potentially pause the stream because of hitting
    // the highWaterMark and pausing the stream, so we resume the stream in the next tick after the underlying onData
    // event has finished
    process.nextTick(() => {
      this.connection.stream.resume();
    });
  }

  handleMessage(message) {
    this.currentMessage = message;
    if (message === code.CopyData) {
      this.rowCount++;
    }
    if (message === code.CopyDone) {
      this.metCopyDone = true;
    }
  }

  handleContent(chunk) {
    if (this.currentMessage === code.CopyData) {
      this.contentChunks.push(chunk);
    }
  }

  pushAccumulatedContentChunks() {
    this.push(Buffer.concat(this.contentChunks));
    this.chunks = [];
  }

  _transform(chunk, enc, cb) {
    this.contentChunks = [];
    this.protocolReader.read(chunk);
    this.pushAccumulatedContentChunks();

    if (this.metCopyDone) {
      this._detach();
      this.push(null);
    }

    cb();
  }

  handleError(e) {
    this.emit("error", e);
  }

  handleCopyData(chunk) {}

  handleCommandComplete() {}

  handleReadyForQuery() {}
}

module.exports = function (txt, options) {
  return new CopyStreamQuery(txt, options);
};
