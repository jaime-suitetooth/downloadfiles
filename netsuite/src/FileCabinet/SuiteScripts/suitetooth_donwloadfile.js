/**
 * @NApiVersion 2.1
 * @NScriptType Restlet
 */
define(['N/file'],
    /**
 * @param{file} file
 */
    (file) => {
        /**
         * Defines the function that is executed when a GET request is sent to a RESTlet.
         * @param {Object} requestParams - Parameters from HTTP request URL; parameters passed as an Object (for all supported
         *     content types)
         * @returns {string | Object} HTTP response body; returns a string when request Content-Type is 'text/plain'; returns an
         *     Object when request Content-Type is 'application/json' or 'application/xml'
         * @since 2015.2
         */
        const get = (requestParams) => {
            try {
	
                var fileObj = file.load( { id: request.fileID } );
                
                // Create the response.
                var response = {};
                response['info'] = fileObj;
                response['content'] = fileObj.getContents();	
        
                return response;				
                
            } catch (e) {		
                return { 'error': e }			
            }
        }
        
        return {get, }

    });
