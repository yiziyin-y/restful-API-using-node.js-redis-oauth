let redis = require("redis"); // Require Redis
let client = redis.createClient(); // Create a new redis instance
let jschema = require("jsonschema");
let Plan = jschema.Validator;
let etag = require("etag");
let schema = require("../schema/schema.json");
let schemaValidator = new Plan();
const elasticsearch = require('elasticsearch');

const esClient = new elasticsearch.Client({
  host: '127.0.0.1:9200',
  log: 'error'
});

const { Kafka } = require('kafkajs');
const { datacatalog_v1beta1 } = require("googleapis");

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})


exports.kafkaProduce = function (data) {
  const producer = kafka.producer();
  const run = async () => {
    // Producing
    await producer.connect()
    await producer.send({
      topic: 'test-topic',
      messages: [
        { value: JSON.stringify(data) },
      ],
    })
    await producer.disconnect()

  }
  run()
};

var out = "";
exports.kafkaConsume = async () => {

  const consumer = kafka.consumer({ groupId: 'test-group' })
  // const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      out = message.value.toString();
      return out;
    },
  })
};

exports.authorization = function (req, res) {
  var auth = false;
  if (!req.headers.authorization)
    throw "Id token is not detected! Please use token to access";

  let token = req.headers.authorization;
  var parts = token.split(".");
  var headerBuf = new Buffer.from(parts[0], "base64");
  var bodyBuf = new Buffer.from(parts[1], "base64");
  var header = JSON.parse(headerBuf.toString());
  var body = JSON.parse(bodyBuf.toString());

  if (
    body.aud !==
    "415227244625-5j07jei9nmdemhaqgmps9norg9fu0f5m.apps.googleusercontent.com" ||
    body.iss !== "https://accounts.google.com"
  ) {
    res.status(401);
    throw "Id token is not valid";
  } else {
    auth = true;
    console.log("Successfully authenticated");
  }
  return auth;
};


exports.get_all_users = (req, res, next) => {
  this.authorization(req, res);

  client.hgetall("HealthPlan", function (err, obj) {
    res.json(obj);
  });
};

exports.add_user = (req, res, next) => {
  this.authorization(req, res);

  var validate = schemaValidator.validate(req.body, schema);
  if (validate.errors.length > 0) {
    var errmsg = [];
    validate.errors.forEach((element) => {
      errmsg.push(element.stack);
    });
    res.send(errmsg);
  }
  let bigId = req.body.objectId;
  let pcsid = req.body.planCostShares.objectId;
  let reqBody = JSON.stringify(req.body);
  let lps = req.body.linkedPlanServices;
  var arr = Array.from(lps);
  let lpsid1 = arr[0].objectId;
  let lpsid2 = arr[1].objectId;
  let ls = arr[0].linkedService;
  let psc = arr[0].planserviceCostShares;
  let lsid = arr[0].linkedService.objectId;
  let pscid = arr[0].planserviceCostShares.objectId;
  let ls2 = arr[1].linkedService;
  let psc2 = arr[1].planserviceCostShares;
  let ls2id = arr[1].linkedService.objectId;
  let psc2id = arr[1].planserviceCostShares.objectId;

  var bigMap = {
    pcsid: pcsid,
    lpsid1: lpsid1,
    lpsid2: lpsid2,
    lsid: lsid,
    ls2id: ls2id,
    psc2id: psc2id,
    objectId: bigId,
    _org: req.body._org,
    objectType: req.body.objectType,
    planType: req.body.planType,
    creationDate: req.body.creationDate,
    plan_service: {
      name: "plan"
    }
  };
  var lpsid1Map = {
    lsid: lsid,
    pscid: pscid,
    objectId: lpsid1,
    _org: arr[0]._org,
    objectType: arr[0].objectType,
    plan_service: {
      parent: bigId,
      name: "planservice"
    }
  };
  var lpsid2Map = {
    ls2id: ls2id,
    psc2id: psc2id,
    objectId: lpsid2,
    _org: arr[1]._org,
    objectType: arr[1].objectType,
    plan_service: {
      parent: bigId,
      name: "planservice"
    }
  };
  var pcs = req.body.planCostShares;
  pcs["plan_service"] = {
    parent: bigId,
    name: "membercostshare"
  }
  ls["plan_service"] = {
    parent: lpsid1,
    name: "service"
  }
  psc["plan_service"] = {
    parent: lpsid1,
    name: "planservice_membercostshare"
  }
  ls2["plan_service"] = {
    parent: lpsid2,
    name: "service"
  }
  psc2["plan_service"] = {
    parent: lpsid2,
    name: "planservice_membercostshare"
  }

  var rep = client.hmset(
    "HealthPlan",
    bigId,
    JSON.stringify(bigMap),
    pcsid,
    JSON.stringify(pcs),
    lpsid1,
    JSON.stringify(lpsid1Map),
    lpsid2,
    JSON.stringify(lpsid2Map),
    lsid,
    JSON.stringify(ls),
    pscid,
    JSON.stringify(psc),
    ls2id,
    JSON.stringify(ls2),
    psc2id,
    JSON.stringify(psc2),
    redis.print
  );

  //Etag Part Begin
  if (rep) {
    res.setHeader("ETag", etag(reqBody));
    console.log(etag(reqBody));
    res.send(
      "Post To Redis Success!" +
      "  objectID:" +
      req.body.objectId +
      "   objectType:" +
      req.body.objectType
    );
  }

  //ElasticSearch Part Begin
  const docs = [];
  const bigIndex = {
    index: {
      _index: 'plan',
      _type: '_doc',
      _id: bigId,
    }
  };
  docs.push(bigIndex);
  docs.push(bigMap);

  data = [pcs, lpsid1Map, lpsid2Map, ls, psc, ls2, psc2]

  for (const quote of data) {
    const esAction = {
      index: {
        _index: 'plan',
        _type: '_doc',
        _id: quote.objectId,
        routing: quote.plan_service.parent
      }
    };

    docs.push(esAction);
    docs.push(quote);
  }

  console.log(docs);

  this.kafkaProduce(docs);
  this.kafkaConsume();

  esClient.bulk({ body: docs }).then(response => {
    let errorCount = 0;
    response.items.forEach(item => {
      if (item.index && item.index.error) {
        console.log(++errorCount, item.index.error);
      }
    });
    console.log(
      `Successfully indexed ${data.length - errorCount}
         out of ${data.length} items`
    );
  })
    .catch(console.err);

};

exports.delete_user = (req, res, next) => {
  this.authorization(req, res);
  // find key associated with the id and deletes it
  client.del("HealthPlan", req.params.id, (err, reply) => {
    if (err) {
      console.log(err); // callback incase something goes wrong
    }
    // log success message
    res.send("Delete By ID Successed !"); // response back to the client
  });
};

exports.delete_all = (req, res, next) => {
  this.authorization(req, res);
  // find key associated with the id and deletes it
  client.del("HealthPlan", (err, reply) => {
    if (err) {
      console.log(err); // callback incase something goes wrong
    }
    // log success message
    res.send("Delete Whole Plan Successed !"); // response back to the client
  });
};

exports.get_user = (req, res, next) => {
  this.authorization(req, res);
  let id = '"' + req.params.id.trim() + '"';
  console.log(id);
  //get all values associated with the key as id
  client.hget("HealthPlan", req.params.id, (error, result) => {
    if (error) {
      res.send(err);
    } else {
      if (result == null) {
        res.send("User does not exist");
      } else {
        res.json(JSON.parse(result));
      }
    }
  });
};

exports.update_user = (req, res, next) => {
  this.authorization(req, res);

  // put Parameters
  var validate = schemaValidator.validate(req.body, schema);
  if (validate.errors.length > 0) {
    var errmsg = [];
    validate.errors.forEach((element) => {
      errmsg.push(element.stack);
    });
    res.send(errmsg);
  }
  
  let id = req.params.id.trim();
  client.hget("HealthPlan", id, (error, result) => {
    if (error) {
      res.send(err);
    } else {
      if (result == null) {
        console.log("here");
        res.send("this ID does not exist");
      } else {
        if (etag(result) == etag(JSON.stringify(req.body))) {
          res.status(304);
        }
        let bigId = req.body.objectId;
        let pcsid = req.body.planCostShares.objectId;
        let reqBody = JSON.stringify(req.body);
        let lps = req.body.linkedPlanServices;
        var arr = Array.from(lps);
        let lpsid1 = arr[0].objectId;
        let lpsid2 = arr[1].objectId;
        let ls = arr[0].linkedService;
        let psc = arr[0].planserviceCostShares;
        let lsid = arr[0].linkedService.objectId;
        let pscid = arr[0].planserviceCostShares.objectId;
        let ls2 = arr[1].linkedService;
        let psc2 = arr[1].planserviceCostShares;
        let ls2id = arr[1].linkedService.objectId;
        let psc2id = arr[1].planserviceCostShares.objectId;

        var bigMap = {
          pcsid: pcsid,
          lpsid1: lpsid1,
          lpsid2: lpsid2,
          lsid: lsid,
          ls2id: ls2id,
          psc2id: psc2id,
          objectId: bigId,
          _org: req.body._org,
          objectType: req.body.objectType,
          planType: req.body.planType,
          creationDate: req.body.creationDate,
          plan_service: {
            name: "plan"
          }
        };
        var lpsid1Map = {
          lsid: lsid,
          pscid: pscid,
          objectId: lpsid1,
          _org: arr[0]._org,
          objectType: arr[0].objectType,
          plan_service: {
            parent: bigId,
            name: "planservice"
          }
        };
        var lpsid2Map = {
          ls2id: ls2id,
          psc2id: psc2id,
          objectId: lpsid2,
          _org: arr[1]._org,
          objectType: arr[1].objectType,
          plan_service: {
            parent: bigId,
            name: "planservice"
          }
        };
        var pcs = req.body.planCostShares;
        pcs["plan_service"] = {
          parent: bigId,
          name: "membercostshare"
        }
        ls["plan_service"] = {
          parent: lpsid1,
          name: "service"
        }
        psc["plan_service"] = {
          parent: lpsid1,
          name: "planservice_membercostshare"
        }
        ls2["plan_service"] = {
          parent: lpsid2,
          name: "service"
        }
        psc2["plan_service"] = {
          parent: lpsid2,
          name: "planservice_membercostshare"
        }
        var rep = client.hmset(
          "HealthPlan",
          bigId,
          JSON.stringify(bigMap),
          pcsid,
          JSON.stringify(req.body.planCostShares),
          lpsid1,
          JSON.stringify(lpsid1Map),
          lpsid2,
          JSON.stringify(lpsid2Map),
          //lpsid2, JSON.stringify(arr[1]),
          lsid,
          JSON.stringify(ls),
          pscid,
          JSON.stringify(psc),
          ls2id,
          JSON.stringify(ls2),
          psc2id,
          JSON.stringify(psc2),
          redis.print
        );

        if (rep) {
          res.setHeader("ETag", etag(reqBody));
          console.log(etag(reqBody));
          res.send(
            "Post To Redis Success!" +
            "  objectID:" +
            req.body.objectId +
            "   objectType:" +
            req.body.objectType
          );
        }
        //ElasticSearch Part Begin
        const docs = [];
        const bigIndex = {
          index: {
            _index: 'plan',
            _type: '_doc',
            _id: bigId,
          }
        };
        docs.push(bigIndex);
        docs.push(bigMap);


        data = [pcs, lpsid1Map, lpsid2Map, ls, psc, ls2, psc2]


        for (const quote of data) {
          const esAction = {
            index: {
              _index: 'plan',
              _type: '_doc',
              _id: quote.objectId,
              routing: quote.plan_service.parent
            }
          };

          docs.push(esAction);
          docs.push(quote);
        }

        console.log(docs);

        esClient.bulk({ body: docs }).then(response => {
          let errorCount = 0;
          response.items.forEach(item => {
            if (item.index && item.index.error) {
              console.log(++errorCount, item.index.error);
            }
          });
          console.log(
            `Successfully indexed ${data.length - errorCount}
       out of ${data.length} items`
          );
        })
          .catch(console.err);
      }
    }
  });
};


exports.patch_user = (req, res, next) => {
  this.authorization(req, res);

  let id = req.params.id.trim();

  client.hget("HealthPlan", id, (error, result) => {
    if (error) {
      res.send(err);
    } else {
      if (result == null) {
        res.send("this ID does not exist");
      } else {
        let keys = Object.keys(req.body);
        let newID = req.body[keys[0]].objectId;
        let reqBody = JSON.stringify(req.body);
        if (etag(result) == etag(JSON.stringify(req.body))) {
          res.status(304);
        }

        var rep = client.hmset("HealthPlan", newID, reqBody);

        //ES Part begin
        const docs = [];
        var data = req.body;
        var first = Object.keys(data)[0];
        var data2 = data[first];

        const esAction = {
          index: {
            _index: 'plan',
            _type: '_doc',
            _id: data2.objectId,
            routing: data2.plan_service.parent
          }
        };

        docs.push(esAction);
        docs.push(data2);
        esClient.bulk({ body: docs }).then(response => {
          let errorCount = 0;
          response.items.forEach(item => {
            if (item.index && item.index.error) {
              console.log(++errorCount, item.index.error);
            }
          });
          console.log(
            `Successfully indexed ${data.length - errorCount}
       out of ${data.length} items`
          );
        })
          .catch(console.err);
        console.log(data);

        if (rep) {
          res.setHeader("ETag", etag(reqBody));
          res.send(
            "Post To Redis Success!" +
            "  objectID:" +
            id +
            "   objectType:" +
            req.body.objectType
          );
        }
      }
    }
  });
};
