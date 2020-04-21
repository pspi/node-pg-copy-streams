const Benchmark = require("benchmark");
const cp = require("duplex-child-process");
const pg = require("pg");

const copy = require("../").from;

const client = function () {
  const client = new pg.Client();
  client.connect();
  return client;
};

const psql = "/opt/postgresql-9.6.1/bin/psql";
const limit = 999999;
const inStream = function () {
  return cp.spawn("seq", ["0", String(limit)]);
};
const suite = new Benchmark.Suite();
suite
  .add({
    name: "unix pipe into psql COPY",
    defer: true,
    fn(d) {
      const c = client();
      c.query("DROP TABLE IF EXISTS plugnumber", () => {
        c.query("CREATE TABLE plugnumber (num int)", () => {
          c.end();
          const from = cp.spawn("sh", ["-c", "seq 0 " + limit + " | " + psql + " postgres -c 'COPY plugnumber FROM STDIN'"]);
          from.on("close", () => {
            d.resolve();
          });
        });
      });
    },
  })
  .add({
    name: "pipe into psql COPY",
    defer: true,
    fn(d) {
      const c = client();
      c.query("DROP TABLE IF EXISTS plugnumber", () => {
        c.query("CREATE TABLE plugnumber (num int)", () => {
          c.end();
          const seq = inStream();
          const from = cp.spawn(psql, ["postgres", "-c", "COPY plugnumber FROM STDIN"]);
          seq.pipe(from);
          from.on("close", () => {
            d.resolve();
          });
        });
      });
    },
  })
  .add({
    name: "pipe into pg-copy-stream COPY",
    defer: true,
    fn(d) {
      const c = client();
      c.query("DROP TABLE IF EXISTS plugnumber", () => {
        c.query("CREATE TABLE plugnumber (num int)", () => {
          const seq = inStream();
          const from = c.query(copy("COPY plugnumber FROM STDIN"));
          seq.pipe(from);
          from.on("end", () => {
            c.end();
            d.resolve();
          });
        });
      });
    },
  })

  .on("cycle", (event) => {
    console.log(String(event.target));
  })
  .on("complete", function () {
    console.log("Fastest is " + this.filter("fastest").map("name"));
  });

const c = client();
c.query("DROP TABLE IF EXISTS plugnumber", () => {
  c.end();
  suite.run();
});
