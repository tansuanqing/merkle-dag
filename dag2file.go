package merkledag 
import (
	"encoding/json" 
	"strings" 
)

func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte { // Hash2File 函数，根据哈希和路径返回文件内容
	// 根据 hash 和 path， 返回对应的文件, hash 对应的类型是 tree
	flag, _ := store.Has(hash) // 检查哈希是否存在
	if flag { 
		objBinary, _ := store.Get(hash) 
		var obj Object
		json.Unmarshal(objBinary, &obj) // 反序列化对象
		pathArr := strings.Split(path, "/") // 按 "/" 分割路径
		cur := 1
		return getFileByDir(obj, pathArr, cur, store) // 根据路径获取文件内容
	}
	return nil 
}

func getFileByDir(obj Object, pathArr []string, cur int, store KVStore) []byte { // 根据文件夹对象和路径获取文件内容
	if cur >= len(pathArr) { // 如果当前位置超出路径长度
		return nil 
	}
	index := 0
	for i := range obj.Links { // 遍历链接列表
		objType := string(obj.Data[index : index+4]) // 类型
		index += 4
		objInfo := obj.Links[i] // 信息
		if objInfo.Name != pathArr[cur] { // 名称路径当前位置不匹配
			continue 
		}
		switch objType {
		case TREE: 
			objDirBinary, _ := store.Get(objInfo.Hash) // 获取文件夹对象的二进制数据
			var objDir Object 
			json.Unmarshal(objDirBinary, &objDir) 
			ans := getFileByDir(objDir, pathArr, cur+1, store)
			if ans != nil { 
				return ans 
			}
		case BLOB: // 二进制文件
			ans, _ := store.Get(objInfo.Hash) 
			return ans 
		case LIST: 
			objLinkBinary, _ := store.Get(objInfo.Hash) // 获取列表对象的二进制数据
			var objLink Object 
			json.Unmarshal(objLinkBinary, &objLink) 
			ans := getFileByList(objLink, store) 
			return ans 
		}
	}
	return nil 
}

func getFileByList(obj Object, store KVStore) []byte { // 根据列表对象获取文件内容
	ans := make([]byte, 0) // 初始化结果字节切片
	index := 0
	for i := range obj.Links { // 遍历列表对象的链接列表
		curObjType := string(obj.Data[index : index+4]) 
		index += 4
		curObjLink := obj.Links[i] 
		curObjBinary, _ := store.Get(curObjLink.Hash) 
		var curObj Object 
		json.Unmarshal(curObjBinary, &curObj) 
		if curObjType == BLOB { 
			ans = append(ans, curObjBinary...) 
		} else { 
			tmp := getFileByList(curObj, store) 
			ans = append(ans, tmp...) 
		}
	}
	return ans 


