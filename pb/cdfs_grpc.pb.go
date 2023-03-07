// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.22.0
// source: cdfs.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// MasterClient is the client API for Master service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MasterClient interface {
	GetLocation(ctx context.Context, in *GetLocationReq, opts ...grpc.CallOption) (*GetLocationResp, error)
	AppendFile(ctx context.Context, in *AppendFileReq, opts ...grpc.CallOption) (*AppendFileResp, error)
	Delete(ctx context.Context, in *DeleteReq, opts ...grpc.CallOption) (*DeleteStatus, error)
	Create(ctx context.Context, in *CreateReq, opts ...grpc.CallOption) (*CreateResp, error)
	// chunk server <-> Master
	HeartBeat(ctx context.Context, in *HeartBeatPayload, opts ...grpc.CallOption) (*HeartBeatResp, error)
}

type masterClient struct {
	cc grpc.ClientConnInterface
}

func NewMasterClient(cc grpc.ClientConnInterface) MasterClient {
	return &masterClient{cc}
}

func (c *masterClient) GetLocation(ctx context.Context, in *GetLocationReq, opts ...grpc.CallOption) (*GetLocationResp, error) {
	out := new(GetLocationResp)
	err := c.cc.Invoke(ctx, "/pb.Master/GetLocation", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterClient) AppendFile(ctx context.Context, in *AppendFileReq, opts ...grpc.CallOption) (*AppendFileResp, error) {
	out := new(AppendFileResp)
	err := c.cc.Invoke(ctx, "/pb.Master/AppendFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterClient) Delete(ctx context.Context, in *DeleteReq, opts ...grpc.CallOption) (*DeleteStatus, error) {
	out := new(DeleteStatus)
	err := c.cc.Invoke(ctx, "/pb.Master/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterClient) Create(ctx context.Context, in *CreateReq, opts ...grpc.CallOption) (*CreateResp, error) {
	out := new(CreateResp)
	err := c.cc.Invoke(ctx, "/pb.Master/Create", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterClient) HeartBeat(ctx context.Context, in *HeartBeatPayload, opts ...grpc.CallOption) (*HeartBeatResp, error) {
	out := new(HeartBeatResp)
	err := c.cc.Invoke(ctx, "/pb.Master/HeartBeat", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MasterServer is the server API for Master service.
// All implementations must embed UnimplementedMasterServer
// for forward compatibility
type MasterServer interface {
	GetLocation(context.Context, *GetLocationReq) (*GetLocationResp, error)
	AppendFile(context.Context, *AppendFileReq) (*AppendFileResp, error)
	Delete(context.Context, *DeleteReq) (*DeleteStatus, error)
	Create(context.Context, *CreateReq) (*CreateResp, error)
	// chunk server <-> Master
	HeartBeat(context.Context, *HeartBeatPayload) (*HeartBeatResp, error)
	mustEmbedUnimplementedMasterServer()
}

// UnimplementedMasterServer must be embedded to have forward compatible implementations.
type UnimplementedMasterServer struct {
}

func (UnimplementedMasterServer) GetLocation(context.Context, *GetLocationReq) (*GetLocationResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetLocation not implemented")
}
func (UnimplementedMasterServer) AppendFile(context.Context, *AppendFileReq) (*AppendFileResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendFile not implemented")
}
func (UnimplementedMasterServer) Delete(context.Context, *DeleteReq) (*DeleteStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (UnimplementedMasterServer) Create(context.Context, *CreateReq) (*CreateResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Create not implemented")
}
func (UnimplementedMasterServer) HeartBeat(context.Context, *HeartBeatPayload) (*HeartBeatResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HeartBeat not implemented")
}
func (UnimplementedMasterServer) mustEmbedUnimplementedMasterServer() {}

// UnsafeMasterServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MasterServer will
// result in compilation errors.
type UnsafeMasterServer interface {
	mustEmbedUnimplementedMasterServer()
}

func RegisterMasterServer(s grpc.ServiceRegistrar, srv MasterServer) {
	s.RegisterService(&Master_ServiceDesc, srv)
}

func _Master_GetLocation_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetLocationReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServer).GetLocation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.Master/GetLocation",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServer).GetLocation(ctx, req.(*GetLocationReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Master_AppendFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendFileReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServer).AppendFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.Master/AppendFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServer).AppendFile(ctx, req.(*AppendFileReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Master_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.Master/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServer).Delete(ctx, req.(*DeleteReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Master_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.Master/Create",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServer).Create(ctx, req.(*CreateReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Master_HeartBeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartBeatPayload)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServer).HeartBeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.Master/HeartBeat",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServer).HeartBeat(ctx, req.(*HeartBeatPayload))
	}
	return interceptor(ctx, in, info, handler)
}

// Master_ServiceDesc is the grpc.ServiceDesc for Master service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Master_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pb.Master",
	HandlerType: (*MasterServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetLocation",
			Handler:    _Master_GetLocation_Handler,
		},
		{
			MethodName: "AppendFile",
			Handler:    _Master_AppendFile_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _Master_Delete_Handler,
		},
		{
			MethodName: "Create",
			Handler:    _Master_Create_Handler,
		},
		{
			MethodName: "HeartBeat",
			Handler:    _Master_HeartBeat_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cdfs.proto",
}

// ChunkServerClient is the client API for ChunkServer service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ChunkServerClient interface {
	// Client -> ChunkServer
	Read(ctx context.Context, in *ReadReq, opts ...grpc.CallOption) (*ReadResp, error)
	AppendData(ctx context.Context, in *AppendDataReq, opts ...grpc.CallOption) (*AppendDataResp, error)
	// ChunkServer -> ChunkServer
	Replicate(ctx context.Context, in *ReplicateReq, opts ...grpc.CallOption) (*ReplicateResp, error)
}

type chunkServerClient struct {
	cc grpc.ClientConnInterface
}

func NewChunkServerClient(cc grpc.ClientConnInterface) ChunkServerClient {
	return &chunkServerClient{cc}
}

func (c *chunkServerClient) Read(ctx context.Context, in *ReadReq, opts ...grpc.CallOption) (*ReadResp, error) {
	out := new(ReadResp)
	err := c.cc.Invoke(ctx, "/pb.ChunkServer/Read", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chunkServerClient) AppendData(ctx context.Context, in *AppendDataReq, opts ...grpc.CallOption) (*AppendDataResp, error) {
	out := new(AppendDataResp)
	err := c.cc.Invoke(ctx, "/pb.ChunkServer/AppendData", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chunkServerClient) Replicate(ctx context.Context, in *ReplicateReq, opts ...grpc.CallOption) (*ReplicateResp, error) {
	out := new(ReplicateResp)
	err := c.cc.Invoke(ctx, "/pb.ChunkServer/Replicate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ChunkServerServer is the server API for ChunkServer service.
// All implementations must embed UnimplementedChunkServerServer
// for forward compatibility
type ChunkServerServer interface {
	// Client -> ChunkServer
	Read(context.Context, *ReadReq) (*ReadResp, error)
	AppendData(context.Context, *AppendDataReq) (*AppendDataResp, error)
	// ChunkServer -> ChunkServer
	Replicate(context.Context, *ReplicateReq) (*ReplicateResp, error)
	mustEmbedUnimplementedChunkServerServer()
}

// UnimplementedChunkServerServer must be embedded to have forward compatible implementations.
type UnimplementedChunkServerServer struct {
}

func (UnimplementedChunkServerServer) Read(context.Context, *ReadReq) (*ReadResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Read not implemented")
}
func (UnimplementedChunkServerServer) AppendData(context.Context, *AppendDataReq) (*AppendDataResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendData not implemented")
}
func (UnimplementedChunkServerServer) Replicate(context.Context, *ReplicateReq) (*ReplicateResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Replicate not implemented")
}
func (UnimplementedChunkServerServer) mustEmbedUnimplementedChunkServerServer() {}

// UnsafeChunkServerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ChunkServerServer will
// result in compilation errors.
type UnsafeChunkServerServer interface {
	mustEmbedUnimplementedChunkServerServer()
}

func RegisterChunkServerServer(s grpc.ServiceRegistrar, srv ChunkServerServer) {
	s.RegisterService(&ChunkServer_ServiceDesc, srv)
}

func _ChunkServer_Read_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChunkServerServer).Read(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.ChunkServer/Read",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChunkServerServer).Read(ctx, req.(*ReadReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _ChunkServer_AppendData_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendDataReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChunkServerServer).AppendData(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.ChunkServer/AppendData",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChunkServerServer).AppendData(ctx, req.(*AppendDataReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _ChunkServer_Replicate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicateReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChunkServerServer).Replicate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.ChunkServer/Replicate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChunkServerServer).Replicate(ctx, req.(*ReplicateReq))
	}
	return interceptor(ctx, in, info, handler)
}

// ChunkServer_ServiceDesc is the grpc.ServiceDesc for ChunkServer service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ChunkServer_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pb.ChunkServer",
	HandlerType: (*ChunkServerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Read",
			Handler:    _ChunkServer_Read_Handler,
		},
		{
			MethodName: "AppendData",
			Handler:    _ChunkServer_AppendData_Handler,
		},
		{
			MethodName: "Replicate",
			Handler:    _ChunkServer_Replicate_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cdfs.proto",
}