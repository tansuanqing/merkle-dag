package merkledag 

import (
	"encoding/json" 
	"fmt"
	"hash" 
	"math" 
)

const (
	K = 1 << 10 // K 等于 1 左移 10 位
	M = K << 10 
	CHUNK_SIZE = 256 * K 
	MAX_LISTLINE = 4096 
	BLOB = "blob" 
	LIST = "link" 
	TREE = "tree" 
)

type Link struct {
	Name string 
	Hash []byte 
	Size int 
}

type Object struct { 
	Links []Link 
	Data  []byte 

// Add 函数将分片写入到 KVStore 中，并返回 Merkle Root
func Add(store KVStore, node Node, h hash.Hash) []byte { // Add 函数，接收 KVStore、Node 和 hash.Hash 参数，返回字节切片
	obj := &Object{} // 创建 Object 对象
	switch node.Type() {
	case FILE: // 如果是文件类型
		obj = handleFile(node, store, h) 
		break
	case DIR: // 文件夹类型
		obj = handleDir(node, store, h) 
		break
	}
	jsonObj, _ := json.Marshal(obj) 
	return computeHash(jsonObj, h) 
}

// 处理文件，返回一个该文件对应的 obj
func handleFile(node Node, store KVStore, h hash.Hash) *Object { 
	obj := &Object{} 
	FileNode, _ := node.(File) 
	if FileNode.Size() > CHUNK_SIZE { 
		numChunks := math.Ceil(float64(FileNode.Size()) / float64(CHUNK_SIZE)) // 计算分块数量
		height := 0
		tmp := numChunks
		// 计算出要分几层
		for {
			height++
			tmp /= MAX_LISTLINE
			if tmp == 0 {
				break
			}
		}
		obj, _ = dfshandleFile(height, FileNode, store, 0, h) // 递归处理大文件
	} else {
		obj.Data = FileNode.Bytes() // 将文件内容存入 Data
		putObjInStore(obj, store, h, BLOB) // 将对象存入 KVStore
	}
	return obj // 返回对象
}

// 处理文件夹，返回对应的 obj 指针
func handleDir(node Node, store KVStore, h hash.Hash) *Object { // 处理文件夹函数，接收节点、KVStore 和 hash.Hash 参数，返回 Object 指针
	dirNode, _ := node.(Dir) 
	iter := dirNode.It() 
treeObject := &Object{} 
	for iter.Next() { 
		node := iter.Node()
		switch node.Type() { 
		case FILE: 
			file := node.(File) 
			tmp := handleFile(node, store, h) 
			jsonMarshal, _ := json.Marshal(tmp) 
			treeObject.Links = append(treeObject.Links, Link{ 
				Hash: computeHash(jsonMarshal, h),
				Size: int(file.Size()), 
				Name: file.Name(), 
			})
			if tmp.Links == nil {
				treeObject.Data = append(treeObject.Data, []byte(BLOB)...) 
			} else {
				treeObject.Data = append(treeObject.Data, []byte(LIST)...) 
			}
			break
		case DIR:
			dir := node.(Dir) 
			tmp := handleDir(node, store, h) 
			jsonMarshal, _ := json.Marshal(tmp) 
			treeObject.Links = append(treeObject.Links, Link{ 
				Hash: computeHash(jsonMarshal, h),
				Size: int(dir.Size()), 
				Name: dir.Name(), 
			})
			treeObject.Data = append(treeObject.Data, []byte(TREE)...) 
			break
		}
	}
	putObjInStore(treeObject, store, h, LIST) 
	return treeObject 
}

// 处理大文件的方法，递归调用，返回当前生成的 obj 已经处理了多少数据
func dfshandleFile(height int, node File, store KVStore, start int, h hash.Hash) (*Object, int) { 
	obj := &Object{} 
	lendata := 0 // 处理数据量
	if height == 1 { // 如果只有一层
		if len(node.Bytes())-start < CHUNK_SIZE { 
			data := node.Bytes()[start:] // 取剩余数据
			obj.Data = append(obj.Data, data...) 
			lendata = len(data) 
			putObjInStore(obj, store, h, BLOB) 
			return obj, lendata 
		} else { 
			for i := 1; i <= MAX_LISTLINE; i++ {
				end := start + CHUNK_SIZE 
				// 确保不越界
				if end > len(node.Bytes()) { 
					end = len(node.Bytes()) 
				}
				data := node.Bytes()[start:end] 
				blobObj := Object{
					Data:  data,
				}
				putObjInStore(&blobObj, store, h, BLOB) 
				jsonMarshal, _ := json.Marshal(blobObj) 
				obj.Links = append(obj.Links, Link{ 
					Hash: computeHash(jsonMarshal, h), 
					Size: int(len(data)), 
				})
				obj.Data = append(obj.Data, []byte(BLOB)...) 
				lendata += len(data) 
				start += CHUNK_SIZE 
				if start >= len(node.Bytes()) {
					break 
				}
			}
			putObjInStore(obj, store, h, LIST) 
			return obj, lendata 
		}
	} else {
		for i := 1; i <= MAX_LISTLINE; i++ { 
			if start >= len(node.Bytes()) { 
				break 
			tmpObj, tmpLendata := dfshandleFile(height-1, node, store, start, h) 
			lendata += tmpLendata 
			jsonMarshal, _ := json.Marshal(tmpObj) 
			obj.Links = append(obj.Links, Link{ 
				Hash: computeHash(jsonMarshal, h), 
				Size: tmpLendata, 
			})
			if tmpObj.Links == nil { 
				obj.Data = append(obj.Data, []byte(BLOB)...) 
			} else {
				obj.Data = append(obj.Data, []byte(LIST)...) 
			}
			start += tmpLendata 
		}
		putObjInStore(obj, store, h, LIST) 
		return obj, lendata 
	}
}

func computeHash(data []byte, h hash.Hash) []byte { 
	h.Reset() 
	h.Write(data) 
	return h.Sum(nil) 
}

func putObjInStore(obj *Object, store KVStore, h hash.Hash, objType string) { 
	value, err := json.Marshal(obj) 
	if err != nil { 
		fmt.Println("json.Marshal err:", err) 
		return 
	}

	hash := computeHash(value, h) 
	flag, _ := store.Has(hash) 
	if flag {
		return 
	}
	if objType == BLOB { 
		store.Put(hash, obj.Data) 
	} else { 
		store.Put(hash, value) 
	}

}




