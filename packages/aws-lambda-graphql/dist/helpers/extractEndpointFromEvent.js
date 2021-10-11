"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.extractEndpointFromEvent = void 0;
function extractEndpointFromEvent(event) {
    console.log('************* aws-lambda-graphql - extractEndpointFromEvent - event', event);
    return `${event.requestContext.domainName}/${event.requestContext.stage}`;
}
exports.extractEndpointFromEvent = extractEndpointFromEvent;
//# sourceMappingURL=extractEndpointFromEvent.js.map
