

#include "gfserver.h"
#include "proxy-student.h"

#define BUFSIZE 4096

struct MemoryStruct {
	char *data;
	size_t size;
};
/*
 * Taken with references to https://curl.se/libcurl/c/getinmemory.html
 */
size_t write_memory_cb(void *contents, size_t size, size_t nmemb, void *data) {
	size_t realsize = size * nmemb;
	struct MemoryStruct *mem = (struct MemoryStruct *)data;
	
	char *ptr = realloc(mem->data, mem->size + realsize + 1);
	
	mem->data = ptr;
	memcpy(&(mem->data[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->data[mem->size] = 0;
	return realsize;
}
/*
 * Taken with references to https://curl.se/libcurl/c/getinmemory.html
 */
ssize_t handle_with_curl(gfcontext_t *ctx, const char *path, void* arg){
	char *data_dir = arg;
	struct MemoryStruct data;
	data.data = NULL;
	data.size = 0;
	CURL *curl_handle;
	CURLcode result;
	int responseCode;
	char buffer[BUFSIZE];
	strncpy(buffer,data_dir, BUFSIZE);
	strncat(buffer, path, BUFSIZE);

	printf("Full file path: %s\n", buffer);
	size_t bytes_transferred;

	curl_handle = curl_easy_init();
	curl_easy_setopt(curl_handle, CURLOPT_URL, buffer);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_memory_cb);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&data);
	result = curl_easy_perform(curl_handle);
	curl_easy_cleanup(curl_handle);
	if (result == CURLE_HTTP_RETURNED_ERROR) {
		free(data.data);
		bytes_transferred = gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);
		return bytes_transferred;
	}
	else if (result == CURLE_OK) {
		result = curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &responseCode);
		if (responseCode != 200){
			free(data.data);
			bytes_transferred = gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);
			return bytes_transferred;
		}
		gfs_sendheader(ctx, GF_OK, data.size);
		bytes_transferred = gfs_send(ctx, data.data, data.size);
		if (bytes_transferred != data.size){
			printf("error in send Bytes sent: %ld out of %ld\n", bytes_transferred, data.size);
			free(data.data);
			return SERVER_FAILURE;
		}
		printf("successful send: %ld out of %ld\n", bytes_transferred, data.size);
	}
	else {
		printf("curl failed\n %i", result);
		free(data.data);
		return SERVER_FAILURE;
	}

	
	free(data.data);
	return data.size;
}


/*
 * We provide a dummy version of handle_with_file that invokes handle_with_curl
 * as a convenience for linking.  We recommend you simply modify the proxy to
 * call handle_with_curl directly.
 */
ssize_t handle_with_file(gfcontext_t *ctx, const char *path, void* arg){
	return handle_with_curl(ctx, path, arg);
}	
