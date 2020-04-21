"use strict";

const _ = require("lodash");
const assert = require("assert");
const concat = require("concat-stream");
const gonna = require("gonna");
const pg = require("pg");

const copy = require("../").from;

const client = function () {
  const client = new pg.Client();
  client.connect();
  return client;
};

const testStreamUnpipe = function () {
  const fromClient = client();
  fromClient.query("CREATE TEMP TABLE numbers(num int)");

  const stream = fromClient.query(copy("COPY numbers FROM STDIN"));
  const unpipeDone = gonna("unpipe the stream");
  let onEndCalled = false;
  fromClient.connection.stream.on("unpipe", (src) => {
    assert.equal(src, stream);
    assert(!onEndCalled);
    unpipeDone();
  });
  stream.on("end", () => {
    onEndCalled = true;
    fromClient.end();
  });
  stream.end(Buffer.from("1\n"));
};

testStreamUnpipe();

const testConstruction = function () {
  const highWaterMark = 10;
  const stream = copy("COPY numbers FROM STDIN", { highWaterMark: 10, objectMode: true });
  for (let i = 0; i < highWaterMark * 1.5; i++) {
    stream.write("1\t2\n");
  }
  assert(!stream.write("1\t2\n"), "Should correctly set highWaterMark.");
};

testConstruction();

const testRange = function (top) {
  const fromClient = client();
  fromClient.query("CREATE TEMP TABLE numbers(num int, bigger_num int)");

  const txt = "COPY numbers FROM STDIN";
  const stream = fromClient.query(copy(txt));
  for (let i = 0; i < top; i++) {
    stream.write(Buffer.from(String(i) + "\t" + i * 10 + "\n"));
  }
  stream.end();
  const countDone = gonna("have correct count");
  stream.on("end", () => {
    fromClient.query("SELECT COUNT(*) FROM numbers", (err, res) => {
      assert.ifError(err);
      assert.equal(res.rows[0].count, top, "expected " + top + " rows but got " + res.rows[0].count);
      assert.equal(stream.rowCount, top, "expected " + top + " rows but db count is " + stream.rowCount);
      //console.log('found ', res.rows.length, 'rows')
      countDone();
      const firstRowDone = gonna("have correct result");
      assert.equal(stream.rowCount, top, "should have rowCount " + top + " ");
      fromClient.query("SELECT (max(num)) AS num FROM numbers", (err, res) => {
        assert.ifError(err);
        assert.equal(res.rows[0].num, top - 1);
        firstRowDone();
        fromClient.end();
      });
    });
  });
};

testRange(1000);

const testSingleEnd = function () {
  const fromClient = client();
  fromClient.query("CREATE TEMP TABLE numbers(num int)");
  const txt = "COPY numbers FROM STDIN";
  const stream = fromClient.query(copy(txt));
  let count = 0;
  stream.on("end", () => {
    count++;
    assert(count == 1, "`end` Event was triggered " + count + " times");
    if (count == 1) {
      fromClient.end();
    }
  });
  stream.end(Buffer.from("1\n"));
};
testSingleEnd();

const testClientReuse = function () {
  const fromClient = client();
  fromClient.query("CREATE TEMP TABLE numbers(num int)");
  const txt = "COPY numbers FROM STDIN";
  let count = 0;
  const countMax = 2;
  const card = 100000;
  var runStream = function () {
    const stream = fromClient.query(copy(txt));
    stream.on("end", () => {
      count++;
      if (count < countMax) {
        runStream();
      } else {
        fromClient.query("SELECT sum(num) AS s FROM numbers", (err, res) => {
          const total = countMax * card * (card + 1);
          assert.equal(res.rows[0].s, total, "copy-from.ClientReuse wrong total");
          fromClient.end();
        });
      }
    });
    stream.write(Buffer.from(_.range(0, card + 1).join("\n") + "\n"));
    stream.end(Buffer.from(_.range(0, card + 1).join("\n") + "\n"));
  };
  runStream();
};
testClientReuse();
