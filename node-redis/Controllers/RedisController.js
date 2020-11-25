let redis = require("redis"); // Require Redis
let client = redis.createClient(); // Create a new redis instance
let jschema = require("jsonschema");
let Plan = jschema.Validator;
let etag = require("etag");
let schema = require("../schema/schema.json");
let schemaValidator = new Plan();

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
  // var validate = schemaValidator.validate(req.body, schema);
  // if (validate.errors.length > 0) {
  //   var errmsg = [];
  //   validate.errors.forEach((element) => {
  //     errmsg.push(element.stack);
  //   });
  //   res.send(errmsg);
  // }
  // let reqId = JSON.stringify(req.body.objectId);
  // let reqBody = JSON.stringify(req.body);
  // console.log(reqId);
  // var rep = client.hmset("HealthPlan", reqId, reqBody, redis.print);
  // if (rep) {
  //   res.setHeader("ETag", etag(reqBody));
  //   console.log(etag(reqBody));
  //   res.send("Post To Redis Success!" + '  objectID:'+req.body.objectId+ '   objectType:'+req.body.objectType);

  // }

  var validate = schemaValidator.validate(req.body, schema);
  if (validate.errors.length > 0) {
    var errmsg = [];
    validate.errors.forEach((element) => {
      errmsg.push(element.stack);
    });
    res.send(errmsg);
  }
  let bigId = JSON.stringify(req.body.objectId);
  let pcsid = JSON.stringify(req.body.planCostShares.objectId);
  let reqBody = JSON.stringify(req.body);
  let lps = req.body.linkedPlanServices;
  var arr = Array.from(lps);
  let lpsid1 = JSON.stringify(arr[0].objectId);
  let lpsid2 = JSON.stringify(arr[1].objectId);

  let ls = arr[0].linkedService;
  let psc = arr[0].planserviceCostShares;
  let lsid = JSON.stringify(arr[0].linkedService.objectId);
  let pscid = JSON.stringify(arr[0].planserviceCostShares.objectId);
  let ls2 = arr[1].linkedService;
  let psc2 = arr[1].planserviceCostShares;
  let ls2id = JSON.stringify(arr[1].linkedService.objectId);
  let psc2id = JSON.stringify(arr[1].planserviceCostShares.objectId);

  var bigMap = {
    pcsid: pcsid,
    lpsid1: lpsid1,
    lpsid2: lpsid2,
    lsid: lsid,
    ls2id: ls2id,
    psc2id: psc2id,
    _org: req.body._org,
    objectType: req.body.objectType,
    planType: req.body.planType,
    creationDate: req.body.creationDate,
  };
  var lpsid1Map = {
    lsid: lsid,
    pscid: pscid,
    _org: arr[0]._org,
    objectType: arr[0].objectType,
  };
  var lpsid2Map = {
    ls2id: ls2id,
    psc2id: psc2id,
    _org: arr[1]._org,
    objectType: arr[1].objectType,
  };

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
  let id = '"' + req.params.id.trim() + '"';
  client.hget("HealthPlan", id, (error, result) => {
    if (error) {
      res.send(err);
    } else {
      if (result == null) {
        console.log("here");
        res.send("User does not exist");
      } else {
        let bigId = JSON.stringify(req.body.objectId);
        let pcsid = JSON.stringify(req.body.planCostShares.objectId);
        let reqBody = JSON.stringify(req.body);
        let lps = req.body.linkedPlanServices;
        var arr = Array.from(lps);
        let lpsid1 = JSON.stringify(arr[0].objectId);
        let lpsid2 = JSON.stringify(arr[1].objectId);

        let ls = arr[0].linkedService;
        let psc = arr[0].planserviceCostShares;
        let lsid = JSON.stringify(arr[0].linkedService.objectId);
        let pscid = JSON.stringify(arr[0].planserviceCostShares.objectId);
        let ls2 = arr[1].linkedService;
        let psc2 = arr[1].planserviceCostShares;
        let ls2id = JSON.stringify(arr[1].linkedService.objectId);
        let psc2id = JSON.stringify(arr[1].planserviceCostShares.objectId);

        var bigMap = {
          pcsid: pcsid,
          lpsid1: lpsid1,
          lpsid2: lpsid2,
          lsid: lsid,
          ls2id: ls2id,
          psc2id: psc2id,
          _org: req.body._org,
          objectType: req.body.objectType,
          planType: req.body.planType,
          creationDate: req.body.creationDate,
        };
        var lpsid1Map = {
          lsid: lsid,
          pscid: pscid,
          _org: arr[0]._org,
          objectType: arr[0].objectType,
        };
        var lpsid2Map = {
          ls2id: ls2id,
          psc2id: psc2id,
          _org: arr[1]._org,
          objectType: arr[1].objectType,
        };

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
      }
    }
  });
};


exports.patch_user = (req, res, next) => {
  this.authorization(req, res);

  let id = '"' + req.params.id.trim() + '"';

  client.hget("HealthPlan", id, (error, result) => {
    if (error) {
      res.send(err);
    } else {
      if (result == null) {
        res.send("User does not exist");
      } else {
        let keys = Object.keys(req.body);
        let newID = JSON.stringify(req.body[keys[0]].objectId);
        let reqBody = JSON.stringify(req.body);
        console.log(JSON.stringify(req.body));
        console.log(result);
        if (etag(result) == etag(JSON.stringify(req.body))) {
          console.log("here");
          res.status(304);
        }

        var rep = client.hmset("HealthPlan", newID, reqBody);
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
