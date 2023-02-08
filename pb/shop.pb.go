// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.20.3
// source: shop.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Shop struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id  int64   `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Lat float64 `protobuf:"fixed64,2,opt,name=lat,proto3" json:"lat,omitempty"`
	Lon float64 `protobuf:"fixed64,3,opt,name=lon,proto3" json:"lon,omitempty"`
}

func (x *Shop) Reset() {
	*x = Shop{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shop_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Shop) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Shop) ProtoMessage() {}

func (x *Shop) ProtoReflect() protoreflect.Message {
	mi := &file_shop_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Shop.ProtoReflect.Descriptor instead.
func (*Shop) Descriptor() ([]byte, []int) {
	return file_shop_proto_rawDescGZIP(), []int{0}
}

func (x *Shop) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Shop) GetLat() float64 {
	if x != nil {
		return x.Lat
	}
	return 0
}

func (x *Shop) GetLon() float64 {
	if x != nil {
		return x.Lon
	}
	return 0
}

type ShopEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Shops []*Shop `protobuf:"bytes,1,rep,name=shops,proto3" json:"shops,omitempty"`
}

func (x *ShopEntry) Reset() {
	*x = ShopEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shop_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ShopEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ShopEntry) ProtoMessage() {}

func (x *ShopEntry) ProtoReflect() protoreflect.Message {
	mi := &file_shop_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ShopEntry.ProtoReflect.Descriptor instead.
func (*ShopEntry) Descriptor() ([]byte, []int) {
	return file_shop_proto_rawDescGZIP(), []int{1}
}

func (x *ShopEntry) GetShops() []*Shop {
	if x != nil {
		return x.Shops
	}
	return nil
}

var File_shop_proto protoreflect.FileDescriptor

var file_shop_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x73, 0x68, 0x6f, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x62, 0x65,
	0x6e, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x22, 0x3a, 0x0a, 0x04, 0x53, 0x68, 0x6f, 0x70, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x02, 0x69, 0x64, 0x12, 0x10,
	0x0a, 0x03, 0x6c, 0x61, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x01, 0x52, 0x03, 0x6c, 0x61, 0x74,
	0x12, 0x10, 0x0a, 0x03, 0x6c, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x01, 0x52, 0x03, 0x6c,
	0x6f, 0x6e, 0x22, 0x31, 0x0a, 0x09, 0x53, 0x68, 0x6f, 0x70, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12,
	0x24, 0x0a, 0x05, 0x73, 0x68, 0x6f, 0x70, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e,
	0x2e, 0x62, 0x65, 0x6e, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x68, 0x6f, 0x70, 0x52, 0x05,
	0x73, 0x68, 0x6f, 0x70, 0x73, 0x42, 0x12, 0x5a, 0x10, 0x62, 0x65, 0x6e, 0x63, 0x68, 0x5f, 0x65,
	0x6c, 0x61, 0x73, 0x74, 0x69, 0x63, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_shop_proto_rawDescOnce sync.Once
	file_shop_proto_rawDescData = file_shop_proto_rawDesc
)

func file_shop_proto_rawDescGZIP() []byte {
	file_shop_proto_rawDescOnce.Do(func() {
		file_shop_proto_rawDescData = protoimpl.X.CompressGZIP(file_shop_proto_rawDescData)
	})
	return file_shop_proto_rawDescData
}

var file_shop_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_shop_proto_goTypes = []interface{}{
	(*Shop)(nil),      // 0: bench.v1.Shop
	(*ShopEntry)(nil), // 1: bench.v1.ShopEntry
}
var file_shop_proto_depIdxs = []int32{
	0, // 0: bench.v1.ShopEntry.shops:type_name -> bench.v1.Shop
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_shop_proto_init() }
func file_shop_proto_init() {
	if File_shop_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_shop_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Shop); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shop_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ShopEntry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_shop_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_shop_proto_goTypes,
		DependencyIndexes: file_shop_proto_depIdxs,
		MessageInfos:      file_shop_proto_msgTypes,
	}.Build()
	File_shop_proto = out.File
	file_shop_proto_rawDesc = nil
	file_shop_proto_goTypes = nil
	file_shop_proto_depIdxs = nil
}
