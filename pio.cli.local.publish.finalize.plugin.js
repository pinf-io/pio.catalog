
const ASSERT = require("assert");
const PATH = require("path");
const GLOB = require("glob");


exports["publish.finalize"] = function(pio, states) {

    var response = {
        catalogs: {}
    };

    return pio.API.Q.fcall(function() {

        ASSERT.equal(typeof pio._config.env.PATH, "string");
        ASSERT.equal(typeof pio._config.config["pio"].servicesPath, "string");
        ASSERT.equal(typeof pio._config.config["pio.vm"].prefixPath, "string");

        function publishCatalog(catalogName, catalogConfig) {

        	ASSERT.equal(typeof catalogConfig.uuid, "string");

            console.log("Publish catalog: " + catalogName);

            response.catalogs[catalogName] = {};

            function getServices() {
                // TODO: Cache `services` for multiple calls once we track config/fs origin and usage so we can determine
                //       if we need to re-scan.
                var services = [];
                var done = pio.API.Q.resolve();
                catalogConfig.services.forEach(function(servicesPath) {
                    done = pio.API.Q.when(done, function() {
                        return pio.API.Q.denodeify(GLOB)(servicesPath, {
                            cwd: pio._config.config["pio"].servicesPath
                        }).then(function(files) {
                            services = services.concat(files.map(function(filepath) {
                                return filepath;
                            }));
                            return;
                        });
                    });

                });
                return pio.API.Q.when(done, function() {
                    return services;
                });
            }

            return getServices().then(function(services) {

            	var statesById = {};
            	states.forEach(function(state) {
            		statesById[state["pio.service"].id] = state;
            	});

            	var date = new Date();

				var payload = {
					name: catalogName,
					uuid: catalogConfig.uuid,
					revision: [
						date.getUTCFullYear(),
						("0" + date.getUTCMonth()).replace(/^0?(\d{2})$/, "$1"),
						("0" + date.getUTCDate()).replace(/^0?(\d{2})$/, "$1"),
						"-",
						("0" + date.getUTCHours()).replace(/^0?(\d{2})$/, "$1"),
						("0" + date.getUTCMinutes()).replace(/^0?(\d{2})$/, "$1"),
						("0" + date.getUTCSeconds()).replace(/^0?(\d{2})$/, "$1")
					].join(""),
					packages: {},
					env: pio.API.DEEPCOPY(pio._configOriginal.env),
// NOTE: This must NOT be enabled again. Mappings MUST come from extending other descruptors using overlays (not by providing mappings in catalogs!)
//					mappings: pio.API.DEEPMERGE(pio.API.DEEPCOPY(pio._configOriginal.mappings), catalogConfig.mappings || {}),
					config: pio.API.DEEPMERGE(pio.API.DEEPCOPY(pio._configOriginal.config), catalogConfig.config || {}),
					services: {}
				};

				services.forEach(function (serviceId) {
					var serviceIdParts = serviceId.split("/");
					if (
						!pio._config.services[serviceIdParts[0]] ||
						!pio._config.services[serviceIdParts[0]][serviceIdParts[1]]
					) {
						console.log(("Skip inclusion of '" + serviceId + "' in catalog. Service is not configured!").yellow);
						return;
					}
					if (
						pio._config.services[serviceIdParts[0]][serviceIdParts[1]].enabled === false
					) {
						console.log(("Skip inclusion of '" + serviceId + "' in catalog. Service is disabled!").yellow);
						return;
					}
					if (
						!statesById[serviceIdParts[1]] ||
						!statesById[serviceIdParts[1]]["pio.catalog"] ||
						!statesById[serviceIdParts[1]]["pio.catalog"].catalogs ||
						!statesById[serviceIdParts[1]]["pio.catalog"].catalogs[catalogName] ||
						!statesById[serviceIdParts[1]]["pio.catalog"].catalogs[catalogName].services ||
						!statesById[serviceIdParts[1]]["pio.catalog"].catalogs[catalogName].services[serviceIdParts[1]]
					) {
						throw new Error("Service '" + serviceIdParts[1] + "' not found in publish info (`states`)");
					}
					var service = statesById[serviceIdParts[1]]["pio.catalog"].catalogs[catalogName].services[serviceIdParts[1]];
					payload.packages[serviceIdParts[1]] = {
						uuid: service.uuid,
						originalChecksum: service.originalChecksum,
						finalChecksum: service.finalChecksum,
						timestamp: service.timestamp
					};
					if (!payload.services[serviceIdParts[0]]) {
						payload.services[serviceIdParts[0]] = {};
					
					}
					payload.services[serviceIdParts[0]][serviceIdParts[1]] = pio.API.DEEPCOPY(pio._config.services[serviceIdParts[0]][serviceIdParts[1]]);
				});

				console.log(("Publish catalog '" + catalogName + "' based on: " + JSON.stringify(payload, null, 4)).magenta);

	            var commands = [];
	            commands.push('. /opt/bin/activate.sh');
	            commands.push('export PATH=' + pio._config.env.PATH);
	            commands.push('echo "' + JSON.stringify(payload).replace(/(["$])/g, '\\$1') + '" | pio-catalog publish ' + catalogName);

	            return pio._state["pio.deploy"]._call("_runCommands", {
	                commands: commands,
	                cwd: pio._config.config["pio.vm"].prefixPath
	            }).then(function(res) {
	                if (res !== null && res.code !== 0) {
	                    throw new Error("Remote commands exited with code: " + res.code);
	                }

	                console.log(("Published catalog to: " + JSON.stringify(res.objects.catalogUrl, null, 4)).cyan);

	                console.log(("Catalog info: " + JSON.stringify({
						name: payload.name,
						uuid: payload.uuid,
						revision: payload.revision,
						id: PATH.basename(res.objects.catalogUrl),
						url: res.objects.catalogUrl
	                }, null, 4)).cyan);

	                response.catalogs[catalogName].url = res.objects.catalogUrl;

	                return;
	            });
            });
        }

        var done = pio.API.Q.defer();
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
