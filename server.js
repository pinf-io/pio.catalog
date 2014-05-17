
const ASSERT = require("assert");
const PATH = require("path");
const FS = require("fs-extra");
const EXPRESS = require("express");
const AWS = require("aws-sdk");
const WAITFOR = require("waitfor");


exports.main = function(callback) {
	try {

	    var ownConfig = JSON.parse(FS.readFileSync(PATH.join(__dirname, "../.pio.json")));

	    ASSERT.equal(typeof ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH, "string");
	    ASSERT.equal(typeof ownConfig.config.server.allow, "object");

	    var awsS3 = null;
	    if (
	    	ownConfig.env.AWS_ACCESS_KEY &&
	    	ownConfig.env.AWS_SECRET_KEY
	    ) {
		    awsS3 = (new AWS.S3({
		        accessKeyId: ownConfig.env.AWS_ACCESS_KEY,
		        secretAccessKey: ownConfig.env.AWS_SECRET_KEY
		    }));
	    }

	    var app = EXPRESS();

	    app.configure(function() {
	        app.use(EXPRESS.logger());
	        app.use(EXPRESS.cookieParser());
	        app.use(EXPRESS.bodyParser());
	        app.use(EXPRESS.methodOverride());
	        app.use(app.router);
	    });

		// This is a standard route to echo a value specified as a query argument
		// back as a session cookie.
		// TODO: Standardize a route such as this.
	    app.get("/.set-session-cookie", function (req, res, next) {
            if (req.query.sid) {
                res.writeHead(204, {
                    'Set-Cookie': 'x-pio-server-sid=' + req.query.sid,
                    'Content-Type': 'text/plain',
                    'Content-Length': "0"
                });
                return res.end();
            }
            return next();
        });

	    app.get("/catalog/:name/:checksum", function (req, res, next) {

	    	if (!awsS3) {
                res.writeHead(204, {
                    'Content-Type': 'text/plain',
                    'Content-Length': "0"
                });
                return res.end();
	    	}

	    	function isAllowed(callback) {
	    		for (var alias in ownConfig.config.server.allow) {
	    			if (ownConfig.config.server.allow[alias].key === req.headers["x-auth-code"]) {
	    				return callback(null, ownConfig.config.server.allow[alias]);
	    			}
	    		}
	    		return callback(null, false);
	    	}

	    	function getCatalog(allowedConfig, callback) {
		    	var catalogPath = 
		    		ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH + "/" +
		    		"catalogs/" +
		    		req.params["name"] + "/" +
		    		req.params["checksum"] + ".catalog.json";
		    	return FS.exists(catalogPath, function(exists) {
		    		if (!exists) {
		    			return callback(new Error("Catalog does not exist!"));
		    		}
		    		return FS.readJson(catalogPath, function(err, catalog) {
						if (err) return callback(err);
			    		var waitfor = WAITFOR.parallel(function(err) {
			    			if (err) return callback(err);
			    			return callback(null, catalog);
			    		});
			    		for (var packageId in catalog.packages) {
			    			for (var aspect in catalog.packages[packageId].aspects) {
			    				if (allowedConfig.aspects.indexOf(aspect) === -1) {
			    					delete catalog.packages[packageId].aspects[aspect];
			    					continue;
			    				}
				    			waitfor(packageId, aspect, function(packageId, aspect, done) {
				    				var uri = catalog.packages[packageId].aspects[aspect];
				    				uri = uri.replace(/^https:\/\/s3\.amazonaws\.com\//, "");
				    				return awsS3.getSignedUrl("getObject", {
						                Bucket: uri.split("/").shift(),
						                Key: uri.split("/").slice(1).join("/")
						            }, function(err, url) {
						            	if (err) return done(err);
						            	catalog.packages[packageId].aspects[aspect] = url;
						            	return done(null);
						            });
				    			});
			    			}
			    		}
			    		return waitfor();
			    	});
		    	});
	    	}

	    	return isAllowed(function(err, allowedConfig) {
	    		if (err) return next(err);
	    		if (!allowedConfig) {
		    		res.writeHead(403);
		    		return res.end("Forbidden");
	    		}
	    		if (req.headers.etag === req.params["checksum"]) {
		    		res.writeHead(304);
		    		return res.end();
	    		}
		    	return getCatalog(allowedConfig, function(err, catalog) {
		    		if (err) return next(err);
		    		var payload = JSON.stringify(catalog, null, 4);
		    		res.writeHead(200, {
		    			"Content-Type": "application/json",
		    			"Content-Length": payload.length,
		    			"Etag": req.params["checksum"]
		    		});
		    		return res.end(payload);
		    	});
	    	});
	    });

		var server = app.listen(process.env.PORT);

		console.log("Listening at: http://localhost:" + process.env.PORT);

	    return callback(null, {
	        server: server
	    });
	} catch(err) {
		return callback(err);
	}
}

if (require.main === module) {
	return exports.main(function(err) {
		if (err) {
			console.error(err.stack);
			process.exit(1);
		}
		// Keep server running.
	});
}
