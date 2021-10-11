"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBConnectionManager = void 0;
const assert_1 = __importDefault(require("assert"));
const aws_sdk_1 = require("aws-sdk");
const errors_1 = require("./errors");
const helpers_1 = require("./helpers");
const isTTLExpired_1 = require("./helpers/isTTLExpired");
const DEFAULT_TTL = 7200;
/**
 * DynamoDBConnectionManager
 *
 * Stores connections in DynamoDB table (default table name is Connections, you can override that)
 */
class DynamoDBConnectionManager {
    constructor({ apiGatewayManager, connectionsTable = 'Connections', dynamoDbClient, subscriptions, ttl = DEFAULT_TTL, debug = false, }) {
        this.hydrateConnection = async (connectionId, options) => {
            console.log('++++++++++++++++ hydrateConnection');
            console.trace();
            const { retryCount = 0, timeout = 50 } = options || {};
            // if connection is not found, throw so we can terminate connection
            let connection;
            for (let i = 0; i <= retryCount; i++) {
                const result = await this.db
                    .get({
                    TableName: this.connectionsTable,
                    Key: {
                        id: connectionId,
                    },
                })
                    .promise();
                if (result.Item) {
                    // Jump out of loop
                    connection = result.Item;
                    break;
                }
                // wait for another round
                await new Promise((r) => setTimeout(r, timeout));
            }
            if (!connection || isTTLExpired_1.isTTLExpired(connection.ttl)) {
                throw new errors_1.ConnectionNotFoundError(`Connection ${connectionId} not found`);
            }
            return connection;
        };
        this.setConnectionData = async (data, { id }) => {
            await this.db
                .update({
                TableName: this.connectionsTable,
                Key: {
                    id,
                },
                UpdateExpression: 'set #data = :data',
                ExpressionAttributeValues: {
                    ':data': data,
                },
                ExpressionAttributeNames: {
                    '#data': 'data',
                },
            })
                .promise();
        };
        this.registerConnection = async ({ connectionId, endpoint, }) => {
            const connection = {
                id: connectionId,
                data: { endpoint, context: {}, isInitialized: false },
            };
            if (this.debug)
                console.log(`Connected ${connection.id}`, connection.data);
            await this.db
                .put({
                TableName: this.connectionsTable,
                Item: Object.assign({ createdAt: new Date().toString(), id: connection.id, data: connection.data }, (this.ttl === false || this.ttl == null
                    ? {}
                    : {
                        ttl: helpers_1.computeTTL(this.ttl),
                    })),
            })
                .promise();
            return connection;
        };
        this.sendToConnection = async (connection, payload) => {
            console.log('!!! ************* aws-lambda-graphql - DynamoDBConnectionManager - sendToConnection - connection', connection);
            console.log('!!! ************* aws-lambda-graphql - DynamoDBConnectionManager - sendToConnection - payload', payload);
            try {
                await this.createApiGatewayManager(connection.data.endpoint)
                    .postToConnection({ ConnectionId: connection.id, Data: payload }, function(err, data) {
                      if (err) console.log(err, err.stack); // an error occurred
                      else console.log(data);
                    })
                    .promise();
            }
            catch (e) {
                console.log('ERROR ************* aws-lambda-graphql - DynamoDBConnectionManager - sendToConnection - e', e);
                // this is stale connection
                // remove it from DB
                if (e && e.statusCode === 410) {
                    await this.unregisterConnection(connection);
                }
                else {
                    throw e;
                }
            }
        };
        this.unregisterConnection = async ({ id }) => {
          console.log('{{{}}} ************* aws-lambda-graphql - DynamoDBConnectionManager - unregisterConnection - id', id);
          console.trace('unregisterConnection');
            // TEMP FIX - comment the delete 
            await Promise.all([
                this.db
                    .delete({
                    Key: {
                        id,
                    },
                    TableName: this.connectionsTable,
                })
                    .promise(),
                this.subscriptions.unsubscribeAllByConnectionId(id),
            ]);
        };
        this.closeConnection = async ({ id, data }) => {
            if (this.debug)
                console.log('Disconnected ', id);
            await this.createApiGatewayManager(data.endpoint)
                .deleteConnection({ ConnectionId: id })
                .promise();
        };
        assert_1.default.ok(typeof connectionsTable === 'string', 'Please provide connectionsTable as a string');
        assert_1.default.ok(typeof subscriptions === 'object', 'Please provide subscriptions to manage subscriptions.');
        assert_1.default.ok(ttl === false || (typeof ttl === 'number' && ttl > 0), 'Please provide ttl as a number greater than 0 or false to turn it off');
        assert_1.default.ok(dynamoDbClient == null || typeof dynamoDbClient === 'object', 'Please provide dynamoDbClient as an instance of DynamoDB.DocumentClient');
        assert_1.default.ok(apiGatewayManager == null || typeof apiGatewayManager === 'object', 'Please provide apiGatewayManager as an instance of ApiGatewayManagementApi');
        assert_1.default.ok(typeof debug === 'boolean', 'Please provide debug as a boolean');
        this.apiGatewayManager = apiGatewayManager;
        this.connectionsTable = connectionsTable;
        this.db = dynamoDbClient || new aws_sdk_1.DynamoDB.DocumentClient();
        this.subscriptions = subscriptions;
        this.ttl = ttl;
        this.debug = debug;
    }
    /**
     * Creates api gateway manager
     *
     * If custom api gateway manager is provided, uses it instead
     */
    createApiGatewayManager(endpoint) {
      console.log('$$$ ************* aws-lambda-graphql - DynamoDBConnectionManager - createApiGatewayManager - endpoint', endpoint);
        if (this.apiGatewayManager) {
            console.log('$$$ ************* aws-lambda-graphql - DynamoDBConnectionManager - createApiGatewayManager 1 - this.apiGatewayManager', this.apiGatewayManager);
            return this.apiGatewayManager;
        }
        console.log('$$$ ************* aws-lambda-graphql - DynamoDBConnectionManager - createApiGatewayManager - new aws_sdk_1.ApiGatewayManagementApi({ endpoint })');
        this.apiGatewayManager = new aws_sdk_1.ApiGatewayManagementApi({ endpoint });
        console.log('$$$ ************* aws-lambda-graphql - DynamoDBConnectionManager - createApiGatewayManager 2 - this.apiGatewayManager', this.apiGatewayManager);
        return this.apiGatewayManager;
    }
}
exports.DynamoDBConnectionManager = DynamoDBConnectionManager;
//# sourceMappingURL=DynamoDBConnectionManager.js.map
