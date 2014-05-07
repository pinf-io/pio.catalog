
const ASSERT = require("assert");
const PATH = require("path");
const GLOB = require("glob");


exports.publish = function(pio, state) {

    var response = {
        catalogs: {}
    };

    return pio.API.Q.fcall(function() {

        ASSERT.equal(typeof state["pio"].servicesPath, "string");
        ASSERT.equal(typeof state["pio.service.deployment"].path, "string");
        ASSERT.equal(typeof state["pio.service"].id, "string");
        ASSERT.equal(typeof state["pio.service"].uuid, "string");
        ASSERT.equal(typeof state["pio.service"].originalPath, "string");
        ASSERT.equal(typeof state["pio.service"].finalChecksum, "string");
        ASSERT.equal(typeof pio._state["pio.deploy"].isSynced, "function");

        if (!pio._state["pio.deploy"].isSynced(state)) {
            if (state["pio.cli.local"].force) {
                console.log(("Cannot publish service '" + state["pio.service"].id + "'! Latest local changes not uploaded to server! BUT SKIP DUE TO FORCE.").yellow);
            } else {
                return pio.API.Q.reject("Cannot publish service '" + state["pio.service"].id + "'! Latest local changes not uploaded to server!");
            }
        }

        function publishCatalog(catalogName, catalogConfig) {

            function isServiceInCatalog() {
                // TODO: Cache `services` for multiple calls once we track config/fs origin and usage so we can determine
                //       if we need to re-scan.
                var services = [];
                var done = pio.API.Q.resolve();
                catalogConfig.services.forEach(function(servicesPath) {
                    done = pio.API.Q.when(done, function() {
                        return pio.API.Q.denodeify(GLOB)(servicesPath, {
                            cwd: state["pio"].servicesPath
                        }).then(function(files) {
                            services = services.concat(files.map(function(filepath) {
                                return filepath.split("/").pop();
                            }));
                            return;
                        });
                    });

                });
                return pio.API.Q.when(done, function() {
                    return (services.indexOf(state["pio.service"].id) >= 0);
                });
            }

            return isServiceInCatalog().then(function (inCatalog) {
                if (!inCatalog) {
                    console.log(("Skip recording latest revision of service '" + state["pio.service"].id + "' in catalog '" + catalogName + "'. Service is not in catalog.").yellow);
                    return;
                }

                console.log("Publish service '" + state["pio.service"].id + "' to catalog: " + catalogName);

                response.catalogs[catalogName] = {
                    services: {}
                };

                var publishInfoCachePath = PATH.join(state["pio.service"].originalPath, ".pio.cache", "pio.catalog", "catalogs", catalogName + ".json");

                function loadPublishCacheInfo() {
                    return pio.API.Q.denodeify(function(callback) {
                        return pio.API.FS.exists(publishInfoCachePath, function(exists) {
                            if (!exists) {
                                return callback(null, null);
                            }
                            return pio.API.FS.readJson(publishInfoCachePath, callback);
                        });
                    })();
                }

                function savePublishCacheInfo(info) {
                    return pio.API.Q.denodeify(pio.API.FS.outputFile)(publishInfoCachePath, JSON.stringify(info, null, 4));
                }

                return loadPublishCacheInfo().then(function(publishCacheInfo) {
                    if (
                        publishCacheInfo &&
                        publishCacheInfo.id === state["pio.service"].id &&
                        publishCacheInfo.uuid === state["pio.service"].uuid &&
                        publishCacheInfo.finalChecksum === state["pio.service"].finalChecksum
                    ) {
                        if (state["pio.cli.local"].force) {
                            console.log(("Skip recording latest revision of service '" + state["pio.service"].id + "' in catalog. Service has not changed. BUT CONTINUE due to 'state[pio.cli.local].force'").yellow);
                        } else {
                            response.catalogs[catalogName].services[state["pio.service"].id] = publishCacheInfo;
                            console.log(("Skip recording latest revision of service '" + state["pio.service"].id + "' in catalog. Service has not changed.").yellow);
                            return;
                        }
                    }

                    var commands = [];
                    commands.push('. /opt/bin/activate.sh');
                    for (var name in state["pio.service.deployment"].env) {
                        commands.push('export ' + name + '="' + state["pio.service.deployment"].env[name] + '"');
                    }
                    if (state["pio.cli.local"].force) {
                        commands.push('export PIO_FORCE=' + state["pio.cli.local"].force);
                    }
                    commands.push("pio-catalog record " + catalogName);

                    return pio._state["pio.deploy"]._call("_runCommands", {
                        commands: commands,
                        cwd: PATH.join(state["pio.service.deployment"].path, "live")
                    }).then(function(res) {
                        if (res !== null && res.code !== 0) {
                            throw new Error("Remote commands exited with code: " + res.code);
                        }
                        response.catalogs[catalogName].services[state["pio.service"].id] = res.objects.entry;
                        return savePublishCacheInfo(res.objects.entry);
                    });
                });
            });
        }

        var done = pio.API.Q.resolve();
        for (var name in pio._config.config["pio.catalog"].catalogs) {
            done = pio.API.Q.when(done, function() {
                return publishCatalog(name, pio._config.config["pio.catalog"].catalogs[name]);
            });
        }
        return done.then(function() {
            return {
                "pio.catalog": response
            };
        });
    });

}
