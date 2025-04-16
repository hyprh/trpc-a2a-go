// Tencent is pleased to support the open source community by making tRPC available.
//
// Copyright (C) 2025 THL A29 Limited, a Tencent company.
// All rights reserved.
//
// If you have downloaded a copy of the tRPC source code from Tencent,
// please note that tRPC source code is licensed under the  Apache 2.0 License,
// A copy of the Apache 2.0 License is included in this file.

package jsonrpc

// Response represents a JSON-RPC response object.
// Either Result or Error MUST be included, but not both.
type Response struct {
	Message
	// Result is REQUIRED on success.
	// This member MUST NOT exist if there was an error invoking the method.
	// The value of this member is determined by the method invoked on the Server.
	// It's stored as an interface{} and often requires type assertion or
	// further unmarshalling based on the expected method result.
	Result interface{} `json:"result,omitempty"`
	// Error is REQUIRED on error.
	// This member MUST NOT exist if there was no error triggered during invocation.
	// The value for this member MUST be an Object as defined in section 5.1.
	Error *Error `json:"error,omitempty"`
}

// NewResponse creates a new JSON-RPC response with a result.
func NewResponse(id interface{}, result interface{}) *Response {
	return &Response{
		Message: Message{JSONRPC: Version, ID: id},
		Result:  result,
	}
}

// NewErrorResponse creates a new JSON-RPC response with an error.
func NewErrorResponse(id interface{}, err *Error) *Response {
	return &Response{
		Message: Message{JSONRPC: Version, ID: id},
		Error:   err,
	}
}
