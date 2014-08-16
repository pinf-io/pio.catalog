#!/usr/bin/env node

const ASSERT = require("assert");
const PATH = require("path");
const MFS = require("mfs");
const FS = new MFS.FileFS({
    lineinfo: true
});
const Q = require("q");
const S3 = require("s3");
const AWS = require("aws-sdk");
const EXEC = require("child_process").exec;
const COLORS = require("colors");
const WAITFOR = require("waitfor");
const COMMANDER = require("commander");
const DEEPEQUAL = require("deep-equal");
const DEEPMERGE = require("deepmerge");
const DEEPCOPY = require("deepcopy");
const CRYPTO = require("crypto");


COLORS.setTheme({
    error: "red"
});



function getCatalogEntryCachePath(dataBasePath, catalogName, serviceId, serviceChecksum, latest) {
    var entryPath = PATH.join(
        dataBasePath,
        "catalogs",
        catalogName,
        serviceId
    );
    if (latest) {
        entryPath = PATH.join(entryPath, "package.json");
    } else {
        entryPath = PATH.join(entryPath, serviceChecksum + ".package.json");
    }
    return entryPath;
}

function getCatalogCachePath(dataBasePath, catalogName, catalogChecksum, latest) {
    var entryPath = PATH.join(
        dataBasePath,
        "catalogs",
        catalogName
    );
    if (latest) {
        entryPath = PATH.join(entryPath, "catalog.json");
    } else {
        entryPath = PATH.join(entryPath, catalogChecksum + ".catalog.json");
    }
    return entryPath;
}


exports.catalog = function(catalog, options) {

    var ownConfig = JSON.parse(FS.readFileSync(PATH.join(__dirname, "../.pio.json")));

    ASSERT.equal(typeof ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH, "string");

    ASSERT.equal(typeof process.env.PIO_SERVICE_PATH, "string");

    var pioConfig = JSON.parse(FS.readFileSync(PATH.join(process.env.PIO_SERVICE_PATH, "live/.pio.json")));

    ASSERT.equal(typeof pioConfig.config["pio.service"].id, "string");
    ASSERT.equal(typeof pioConfig.config["pio.service"].originalChecksum, "string");
    ASSERT.equal(typeof pioConfig.config["pio.service"].finalChecksum, "string");
    ASSERT.equal(typeof pioConfig.config["pio.service"].uuid, "string");
    ASSERT.equal(typeof pioConfig.config["pio"].serviceRepositoryUri, "string");
    ASSERT.equal(typeof pioConfig.config["pio.service.deployment"].env.AWS_ACCESS_KEY, "string", 'env.AWS_ACCESS_KEY must be set!');
    ASSERT.equal(typeof pioConfig.config["pio.service.deployment"].env.AWS_SECRET_KEY, "string", 'env.AWS_SECRET_KEY must be set!');

    var syncServicePath = FS.realpathSync(PATH.join(process.env.PIO_SERVICE_PATH, "sync"));
    var liveServicePath = FS.realpathSync(PATH.join(process.env.PIO_SERVICE_PATH, "live"));

    var awsS3 = (new AWS.S3({
        accessKeyId: pioConfig.config["pio.service.deployment"].env.AWS_ACCESS_KEY,
        secretAccessKey: pioConfig.config["pio.service.deployment"].env.AWS_SECRET_KEY
    }));

    function exists(cacheUri, callback) {
        return awsS3.headObject({
            Bucket: cacheUri.split("/").shift(),
            Key: cacheUri.split("/").slice(1).join("/")
        }, function (err, response) {
            console.log("response", response);
            if (err) {
                console.log("err.statusCode", err.statusCode);
                console.log("err.message", err.message);
                if (err.statusCode === 404) {
                    return callback(null, false);
                }
                return callback(err);
            }
            /*
            example `response`:
            {
                AcceptRanges: 'bytes',
                ContentLength: '712',
                ContentType: 'application/x-tar',
                ETag: '"310efd6222df8c5908c422e4e16c1297"',
                LastModified: 'Sat, 12 Mar 2014 06:03:46 GMT',
                Metadata: {},
                RequestId: 'CECB2C5FSA256B10'
            }
            */
            return callback(null, true);
        });
    }

    function upload(archivePath, cacheUri, callback) {
        function attempt(count, callback) {
            var uploader = S3.createClient({
                key: pioConfig.config["pio.service.deployment"].env.AWS_ACCESS_KEY,
                secret: pioConfig.config["pio.service.deployment"].env.AWS_SECRET_KEY,
                bucket: cacheUri.split("/").shift()
            }).upload(archivePath, cacheUri.split("/").slice(1).join("/"), {
                'Content-Type': 'application/x-tar',
                'x-amz-acl': 'private'
            });
            uploader.on('error', function(err) {
                console.error("err", err);
                console.error("err.message", err.message);
                console.error("err.code", err.code);
                if (count < 5) {
                    console.log("Trying again in 3 seconds ...");
                    return setTimeout(function() {
                        return attempt(count + 1, callback);
                    }, 3 * 1000);
                }
                return callback(err);
            });
            uploader.on('progress', function(amountDone, amountTotal) {
                console.log("upload progress", amountDone, amountTotal);
            });
            return uploader.on('end', function(url) {
                return callback(null);
            });
        }
        return attempt(1, callback);
    }

    function getDirHash(path, callback) {

        console.log("getDirHash(" + path + ")");

        // TODO: Replace this checksum logic with 1) meta data if available 2) better scanning that does not load files into memory.

        // ----------------
        // @source https://github.com/mcavage/node-dirsum/blob/master/lib/dirsum.js
        // Changes:
        //  * Do not die on non-existent symlink.
        //  * Bugfixes.
        // TODO: Contribute back to author.
        function _summarize(method, hashes) {
          var keys = Object.keys(hashes);
          keys.sort();

          var obj = {};
          obj.files = hashes;
          var hash = CRYPTO.createHash(method);
          for (var i = 0; i < keys.length; i++) {
            if (typeof(hashes[keys[i]]) === 'string') {
              hash.update(hashes[keys[i]]);
            } else if (typeof(hashes[keys[i]]) === 'object') {
              hash.update(hashes[keys[i]].hash);
            } else {
              console.error('Unknown type found in hash: ' + typeof(hashes[keys[i]]));
            }
          }

          obj.hash = hash.digest('hex');
          return obj;
        }

        function digest(root, method, callback) {
            try {
              if (!root || typeof(root) !== 'string') {
                throw new TypeError('root is required (string)');
              }
              if (method) {
                if (typeof(method) === 'string') {
                  // NO-OP
                } else if (typeof(method) === 'function') {
                  callback = method;
                  method = 'md5';
                } else {
                  throw new TypeError('hash must be a string');
                }
              } else {
                throw new TypeError('callback is required (function)');
              }
              if (!callback) {
                throw new TypeError('callback is required (function)');
              }

              var hashes = {};

              FS.readdir(root, function(err, files) {
                if (err) return callback(err);

                if (files.length === 0) {
                  return callback(undefined, {hash: '', files: {}});
                }
                var hashed = 0;
                files.forEach(function(f) {
                  var path = root + '/' + f;
                  FS.stat(path, function(err, stats) {
                    if (err) {
                        if (err.code === "ENOENT") {
                            // We have a symlink that points to target that does not exist.
                            hashes[f] = "na";
                            if (++hashed >= files.length) {
                              return callback(undefined, _summarize(method, hashes));
                            }
                            return;
                        }
                        return callback(err);
                    }
                    if (stats.isDirectory()) {
                      return digest(path, method, function(err, hash) {
                        if (err) return callback(err);

                        hashes[f] = hash;
                        if (++hashed >= files.length) {
                          return callback(undefined, _summarize(method, hashes));
                        }
                      });
                    } else if (stats.isFile()) {
                      FS.readFile(path, 'utf8', function(err, data) {
                        if (err) return callback(err);

                        var hash = CRYPTO.createHash(method);
                        hash.update(data);
                        hashes[f] = hash.digest('hex');

                        if (++hashed >= files.length) {
                          return callback(undefined, _summarize(method, hashes));
                        }
                      });
                    } else {
                      console.error('Skipping hash of %s', f);
                      if (++hashed > files.length) {
                        return callback(undefined, _summarize(method, hashes));
                      }
                    }
                  });
                });
              });
            } catch (err) {
                return callback(err);
            }
        }
        // ----------------

        return digest(path, "sha1", function (err, info) {
            if (err) return callback(err);
            return callback(null, info.hash);
        });
    }

    function cacheUriForType(sourcHash, type) {
        var cacheUri = 
            pioConfig.config["pio"].serviceRepositoryUri + "/" +
            pioConfig.config["pio.service"].id + "-" +
            pioConfig.config["pio.service"].finalChecksum.substring(0, 7) + "-" +
            sourcHash.substring(0, 7) + "-" +
            type;
        if (type === "build") {
            cacheUri += "-" + process.platform + "-" + process.arch
        }
        cacheUri += ".tgz";
        if (!/^https:\/\/s3\.amazonaws\.com\//.test(cacheUri)) {
            throw new Error("'config.pio.serviceRepositoryUri' must begin with 'https://s3.amazonaws.com/'");
        } else {
            cacheUri = cacheUri.replace(/^https:\/\/s3\.amazonaws\.com\//, "");
        }
        return cacheUri;
    }

    function platformifyAspectName(type) {
        if (type === "build") {
            return type + "[platform=" + process.platform + "&arch=" + process.arch + "]";
        }
        return type;
    }


    function catalogType(type) {

        console.log("Cataloging for type: " + type);

        var sourcePath = PATH.join(syncServicePath, type);
        if (!FS.existsSync(sourcePath)) {
            sourcePath = PATH.join(liveServicePath, type);
        }

        if (!FS.existsSync(sourcePath)) {
            return Q.resolve(null);
        }

        return Q.denodeify(getDirHash)(sourcePath).then(function (sourcHash) {

            var cacheUri = cacheUriForType(sourcHash, type);

            var archivePath = sourcePath + ".tgz";

            return Q.denodeify(exists)(cacheUri).then(function(exists) {
                if (exists) {
                    if (options.force) {
                        console.log(("Skip creating archive and upload for '" + sourcePath + "'. Already uploaded! BUT SKIP DUE TO FORCE.").yellow);
                    } else {
                        console.log(("Skip creating archive and upload for '" + sourcePath + "'. Already uploaded!").yellow);
                        return cacheUri;
                    }
                }

                if (!FS.existsSync(sourcePath)) {
                    console.log(("Skip creating archive and upload for '" + sourcePath + "'. Path does not exist!").yellow);
                    return null;
                }

                console.log(("Creating archive '" + archivePath + "' from '" + sourcePath + "'").magenta);

                if (FS.existsSync(archivePath)) {
                    FS.unlinkSync(archivePath);
                }

                return Q.denodeify(function(callback) {
                    var command = '/bin/tar --dereference -zcf "' + PATH.basename(archivePath) + '" -C "' + PATH.dirname(sourcePath) + '/" "' + PATH.basename(sourcePath) + '"';
                    console.log("Running command: " + command + " (cwd: " + PATH.dirname(archivePath) + ")");
                    return EXEC(command, {
                        cwd: PATH.dirname(archivePath)
                    }, function(err, stdout, stderr) {
                        if (err) {
                            console.error("Error creating archive:", err.stack);
                            process.stderr.write(stdout);
                            process.stderr.write(stderr);
                            return callback(err);
                        }
                        console.log("Archive created. Uploading to S3.".magenta);
                        return callback(null);
                    });
                })().then(function() {
                    console.log(("Uploading archive '" + archivePath + "' to '" + cacheUri + "'").magenta);
                    return Q.denodeify(upload)(archivePath, cacheUri).then(function() {
                        console.log("Uploaded archive to: " + cacheUri);
                        return cacheUri;
                    });
                });
            });
        });
    }

    function recordInCatalog(serviceInfo, callback) {

        function getEntryPath(latest) {
            return getCatalogEntryCachePath(
                ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH,
                catalog,
                pioConfig.config["pio.service"].id,
                pioConfig.config["pio.service"].finalChecksum,
                latest
            );
        }

        var entryPath = getEntryPath(true);

        function loadExisting(callback) {
            return FS.exists(entryPath, function(exists) {
                if (!exists) return callback(null);
                return FS.readJson(entryPath, callback);
            });
        }

        return loadExisting(function(err, existingServiceInfo) {
            if (err) return callback(err);

            if (existingServiceInfo) {
                var timestamp = existingServiceInfo.timestamp;
                delete serviceInfo.timestamp;
                delete existingServiceInfo.timestamp;
                if (DEEPEQUAL(serviceInfo, existingServiceInfo)) {
                    existingServiceInfo.timestamp = timestamp;
                    console.log("Existing catalog entry:", '<wf name="entry">', JSON.stringify(existingServiceInfo, null, 4), '</wf>');
                    console.log(("Skip recording in catalog. Nothing has changed!").yellow);
                    return callback(null);
                }
            }

            var newEntry = JSON.stringify(serviceInfo, null, 4);

            console.log("New catalog entry:", '<wf name="entry">', newEntry, '</wf>');

            var path = getEntryPath(false);
            console.log(("Record in catalog FS at '" + path + "'").magenta);
            return FS.outputFile(path, newEntry, function (err) {
                if (err) return callback(err);
                path = entryPath;
                console.log(("Record in catalog FS at '" + path + "'").magenta);
                return FS.outputFile(path, newEntry, callback);
            });
        });
    }

    var serviceInfo = {
        uuid: pioConfig.config["pio.service"].uuid,
        originalChecksum: pioConfig.config["pio.service"].originalChecksum,
        finalChecksum: pioConfig.config["pio.service"].finalChecksum,
        syncInfo: pioConfig.config["pio.service"].syncInfo || null,
        timestamp: Date.now(),
        aspects: {},
        descriptor: DEEPCOPY(pioConfig.config["pio.service"].descriptor) || {}
    };
    // TODO: Remove this once config boundaries are better established.
    if (pioConfig.config["pio.service"].config) {
        serviceInfo.descriptor.config = DEEPMERGE(serviceInfo.descriptor.config || {}, pioConfig.config["pio.service"].config);
    }
    if (pioConfig.config["pio.service"]["config.plugin~raw"]) {
        serviceInfo.descriptor["config.plugin"] = DEEPMERGE(serviceInfo.descriptor["config.plugin"] || {}, pioConfig.config["pio.service"]["config.plugin~raw"]);
    }
    // TODO: This should already be merged by the time we get here.
    if (
        pioConfig.config["pio.service"].sourceDescriptor &&
        pioConfig.config["pio.service"].sourceDescriptor["config.plugin"]
    ) {
        serviceInfo.descriptor["config.plugin"] = DEEPMERGE(pioConfig.config["pio.service"].sourceDescriptor["config.plugin"], serviceInfo.descriptor["config.plugin"] || {});
    }

    // TODO: Get converter to adjust descriptor based on layout info?
    if (
        serviceInfo.descriptor.config &&
        serviceInfo.descriptor.config["pio.deploy.converter"]
    ) {
        serviceInfo.descriptor.config["pio.deploy.converter"].scriptsPath = "scripts";
        serviceInfo.descriptor.config["pio.deploy.converter"].sourcePath = "source";
    }

    var all = [];
    var done = Q.resolve();
    [
        "scripts",
        "source",
        "build"
    ].forEach(function(type) {
        done = Q.when(done, function() {
            return catalogType(type).then(function(uri) {
                if (uri) {
                    serviceInfo.aspects[platformifyAspectName(type)] = "https://s3.amazonaws.com/" + uri;
                }
                return;
            });
        });
    });
    return Q.when(done).then(function() {
        return Q.denodeify(recordInCatalog)(serviceInfo);
    });
}

exports.publish = function(catalogName, options) {

    var ownConfig = JSON.parse(FS.readFileSync(PATH.join(__dirname, "../.pio.json")));

    ASSERT.equal(typeof ownConfig.config["pio"].hostname, "string");
    ASSERT.equal(typeof ownConfig.config["pio.vm"].ip, "string");
    ASSERT.equal(typeof ownConfig.config["pio.service"].id, "string");
    ASSERT.equal(typeof ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH, "string");
    ASSERT.equal(typeof ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_ID_SAFE, "string");
    ASSERT.equal(typeof ownConfig.config.server.port, "number");

    function readPayload() {
        var deferred = Q.defer();
        var data = [];
        process.stdin.resume();
        process.stdin.setEncoding('utf8');
        process.stdin.on('data', function (_data) {
            data.push(_data.toString());
        });
        process.stdin.on('end', function () {
            try {
                data = JSON.parse(data.join(""));
            } catch(err) {
                return deferred.reject(err);
            }
            return deferred.resolve(data);
        });

        return deferred.promise;
    }

    function verifyPackage(id, requestedEntry, callback) {
        var catalogEntryCachePath = getCatalogEntryCachePath(
            ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH,
            catalogName,
            id,
            requestedEntry.finalChecksum,
            false
        );
        return FS.exists(catalogEntryCachePath, function(exists) {
            if (!exists) {
                return callback(new Error("No catalog cache entry found at path: " + catalogEntryCachePath));
            }
            return FS.readJson(catalogEntryCachePath, function(err, cachedEntry) {
                if (err) return callback(err);
                if (
                    cachedEntry.uuid !== requestedEntry.uuid ||
                    cachedEntry.finalChecksum !== requestedEntry.finalChecksum ||
                    cachedEntry.timestamp !== requestedEntry.timestamp
                ) {
                    console.error(cachedEntry.uuid, requestedEntry.uuid);
                    console.error(cachedEntry.finalChecksum, requestedEntry.finalChecksum);
                    return callback(new Error("Catalog cache entry at path '" + catalogEntryCachePath + "' does not match requested entry: " + JSON.stringify(requestedEntry)));
                }
                return callback(null, cachedEntry);
            });
        });
    }

    function buildCatalog(payload) {
        try {

            ASSERT.equal(typeof payload.name, "string", "'payload.name not set in catalog!'");
            ASSERT.equal(typeof payload.uuid, "string", "'payload.uuid not set in catalog!'");
            ASSERT.notEqual(typeof payload.revision, "undefined", "'payload.revision not set in catalog!'");
            ASSERT.equal(typeof payload.config, "object", "'payload.config not set in catalog!'");
            ASSERT.equal(typeof payload.services, "object", "'payload.services not set in catalog!'");

            var catalog = {
                name: payload.name,
                uuid: payload.uuid,
                revision: payload.revision,
                packages: {},
                env: payload.env || {},
                config: payload.config,
                services: payload.services
            };
            var all = [];
            Object.keys(payload.packages).forEach(function(id) {
                all.push(Q.denodeify(verifyPackage)(id, payload.packages[id]).then(function(meta) {
                    catalog.packages[id] = meta;
                }));
            });
            return Q.all(all).then(function() {
                return catalog;
            });
        } catch(err) {
            return Q.reject(err);
        }
    }

    function saveCatalog(path, catalog) {
        console.log(("Save catalog at: " + path).magenta);
        var deferred = Q.defer();
        FS.outputFile(path, JSON.stringify(catalog, null, 4), function (err) {
            if (err) return deferred.reject(err);
            return deferred.resolve();
        });
        return deferred.promise;
    }

    return readPayload().then(function(payload) {

        return buildCatalog(payload).then(function(catalog) {

            console.log(JSON.stringify(catalog, null, 4));

            function generateCatalogChecksum() {
                var key = [
                    catalog.uuid,
                    catalog.revision
                ];
                for (var id in catalog.packages) {
                    key.push(catalog.packages[id].uuid);
                    key.push(catalog.packages[id].finalChecksum);
                }
                var shasum = CRYPTO.createHash("sha1");
                shasum.update(key.join(":"));
                return shasum.digest("hex");
            }

            var catalogChecksum = generateCatalogChecksum();

            return saveCatalog(
                getCatalogCachePath(ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH, catalogName, catalogChecksum, false),
                catalog
            ).then(function() {
                return saveCatalog(
                    getCatalogCachePath(ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_DATA_BASE_PATH, catalogName, catalogChecksum, true),
                    catalog
                );
            }).then(function() {

                /*
                var catalogRequest = {
                    "method": "GET",
                    "hostname": ownConfig.config["pio.vm"].ip,
                    "port": ownConfig.config["pio.catalog"].server.port,
                    "path": "/catalog/" + catalogName + "/" + catalogChecksum,
                    "headers": {
                        "Host": ownConfig.config["pio.service"].id
                    }
                };
                console.log("Catalog Request:", '<wf name="catalogRequest">', JSON.stringify(catalogRequest, null, 4), '</wf>');
                */

                var catalogUrl = 
                    "http://" + 
                    ownConfig.config["pio.service.deployment"].env.PIO_SERVICE_ID_SAFE +
                    "." +
                    ownConfig.config["pio"].hostname +
                    ":" + ownConfig.config.server.port + 
                    "/catalog/" + catalogName + "/" + catalogChecksum;

                console.log("Catalog URL:", '<wf name="catalogUrl">', JSON.stringify(catalogUrl, null, 4), '</wf>');
                return;
            });
        });
    });
}



// NOTE: Should be run with `CWD` set to the root of a deployed PIO service.
if (require.main === module) {

    function error(err) {
        if (typeof err === "string") {
            console.error((""+err).red);
        } else
        if (typeof err === "object" && err.stack) {
            console.error((""+err.stack).red);
        }
        process.exit(1);
    }

    try {

        return Q.denodeify(function(callback) {

            var program = new COMMANDER.Command();

            var options = {
                force: process.env.PIO_FORCE || false,
                verbose: process.env.PIO_VERBOSE || false,
                debug: process.env.PIO_VERBOSE || process.env.PIO_DEBUG || false,
                silent: process.env.PIO_SILENT || false
            };

            if (options.debug) {
                FS.on("used-path", function(path, method, meta) {
                    console.log("[pio.catalog] FS." + method, path, "(" + meta.file + " @ " + meta.line + ")");
                });
            }

            var acted = false;

            program
                .command("record <catalog>")
                .description("Write the catalog entry for the live revision of a service.")
                .action(function(catalog) {
                    acted = true;
                    return exports.catalog(catalog, options).then(function() {
                        return callback(null);
                    }).fail(callback);
                });

            program
                .command("publish <catalog>")
                .description("Publish a catalog for consumption by users.")
                .action(function(catalog) {
                    acted = true;
                    return exports.publish(catalog, options).then(function() {
                        return callback(null);
                    }).fail(callback);
                });

            program.parse(process.argv);

            if (!acted) {
                var command = process.argv.slice(2).join(" ");
                if (command) {
                    console.error(("ERROR: Command '" + process.argv.slice(2).join(" ") + "' not found!").error);
                }
                program.outputHelp();
                return callback(null);
            }
        })().then(function() {
            return process.exit(0);
        }).fail(error);
    } catch(err) {
        return error(err);
    }
}
