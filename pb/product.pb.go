// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.20.3
// source: product.proto

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

type Product struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Sku        string `protobuf:"bytes,1,opt,name=sku,proto3" json:"sku,omitempty"`
	Name       string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	SearchText string `protobuf:"bytes,3,opt,name=search_text,json=searchText,proto3" json:"search_text,omitempty"`
	Field1     string `protobuf:"bytes,4,opt,name=field1,proto3" json:"field1,omitempty"`
	Field2     string `protobuf:"bytes,5,opt,name=field2,proto3" json:"field2,omitempty"`
	Field3     string `protobuf:"bytes,6,opt,name=field3,proto3" json:"field3,omitempty"`
	Field4     string `protobuf:"bytes,7,opt,name=field4,proto3" json:"field4,omitempty"`
	Field5     string `protobuf:"bytes,8,opt,name=field5,proto3" json:"field5,omitempty"`
	Field6     string `protobuf:"bytes,9,opt,name=field6,proto3" json:"field6,omitempty"`
	Field7     string `protobuf:"bytes,10,opt,name=field7,proto3" json:"field7,omitempty"`
	Field8     string `protobuf:"bytes,11,opt,name=field8,proto3" json:"field8,omitempty"`
	Field9     string `protobuf:"bytes,12,opt,name=field9,proto3" json:"field9,omitempty"`
}

func (x *Product) Reset() {
	*x = Product{}
	if protoimpl.UnsafeEnabled {
		mi := &file_product_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Product) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Product) ProtoMessage() {}

func (x *Product) ProtoReflect() protoreflect.Message {
	mi := &file_product_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Product.ProtoReflect.Descriptor instead.
func (*Product) Descriptor() ([]byte, []int) {
	return file_product_proto_rawDescGZIP(), []int{0}
}

func (x *Product) GetSku() string {
	if x != nil {
		return x.Sku
	}
	return ""
}

func (x *Product) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Product) GetSearchText() string {
	if x != nil {
		return x.SearchText
	}
	return ""
}

func (x *Product) GetField1() string {
	if x != nil {
		return x.Field1
	}
	return ""
}

func (x *Product) GetField2() string {
	if x != nil {
		return x.Field2
	}
	return ""
}

func (x *Product) GetField3() string {
	if x != nil {
		return x.Field3
	}
	return ""
}

func (x *Product) GetField4() string {
	if x != nil {
		return x.Field4
	}
	return ""
}

func (x *Product) GetField5() string {
	if x != nil {
		return x.Field5
	}
	return ""
}

func (x *Product) GetField6() string {
	if x != nil {
		return x.Field6
	}
	return ""
}

func (x *Product) GetField7() string {
	if x != nil {
		return x.Field7
	}
	return ""
}

func (x *Product) GetField8() string {
	if x != nil {
		return x.Field8
	}
	return ""
}

func (x *Product) GetField9() string {
	if x != nil {
		return x.Field9
	}
	return ""
}

var File_product_proto protoreflect.FileDescriptor

var file_product_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x08, 0x62, 0x65, 0x6e, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x22, 0xa8, 0x02, 0x0a, 0x07, 0x50, 0x72,
	0x6f, 0x64, 0x75, 0x63, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x73, 0x6b, 0x75, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x73, 0x6b, 0x75, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x73,
	0x65, 0x61, 0x72, 0x63, 0x68, 0x5f, 0x74, 0x65, 0x78, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0a, 0x73, 0x65, 0x61, 0x72, 0x63, 0x68, 0x54, 0x65, 0x78, 0x74, 0x12, 0x16, 0x0a, 0x06,
	0x66, 0x69, 0x65, 0x6c, 0x64, 0x31, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x31, 0x12, 0x16, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x32, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x32, 0x12, 0x16, 0x0a, 0x06,
	0x66, 0x69, 0x65, 0x6c, 0x64, 0x33, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x33, 0x12, 0x16, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x34, 0x18, 0x07,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x34, 0x12, 0x16, 0x0a, 0x06,
	0x66, 0x69, 0x65, 0x6c, 0x64, 0x35, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x35, 0x12, 0x16, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x36, 0x18, 0x09,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x36, 0x12, 0x16, 0x0a, 0x06,
	0x66, 0x69, 0x65, 0x6c, 0x64, 0x37, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x37, 0x12, 0x16, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x38, 0x18, 0x0b,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x38, 0x12, 0x16, 0x0a, 0x06,
	0x66, 0x69, 0x65, 0x6c, 0x64, 0x39, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x39, 0x42, 0x12, 0x5a, 0x10, 0x62, 0x65, 0x6e, 0x63, 0x68, 0x5f, 0x65, 0x6c,
	0x61, 0x73, 0x74, 0x69, 0x63, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_product_proto_rawDescOnce sync.Once
	file_product_proto_rawDescData = file_product_proto_rawDesc
)

func file_product_proto_rawDescGZIP() []byte {
	file_product_proto_rawDescOnce.Do(func() {
		file_product_proto_rawDescData = protoimpl.X.CompressGZIP(file_product_proto_rawDescData)
	})
	return file_product_proto_rawDescData
}

var file_product_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_product_proto_goTypes = []interface{}{
	(*Product)(nil), // 0: bench.v1.Product
}
var file_product_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_product_proto_init() }
func file_product_proto_init() {
	if File_product_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_product_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Product); i {
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
			RawDescriptor: file_product_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_product_proto_goTypes,
		DependencyIndexes: file_product_proto_depIdxs,
		MessageInfos:      file_product_proto_msgTypes,
	}.Build()
	File_product_proto = out.File
	file_product_proto_rawDesc = nil
	file_product_proto_goTypes = nil
	file_product_proto_depIdxs = nil
}
