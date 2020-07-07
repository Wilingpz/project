#pragma once
#define _SCL_SECURE_NO_WARNINGS
#include<iostream>
#include<sstream>
#include<vector>
#include<string>
#include<unordered_map>
#include<boost/filesystem.hpp>
#include<boost/algorithm/string.hpp>//split的头文件
#include"httplib.h"


class FileUtil
{
public:
	static bool Read(const std::string &name, std::string *body)
	{ //从文件中读取所有内容
		std::ifstream fs(name, std::ios::binary);
		if (fs.is_open() == false)
		{
			std::cout << "open file " << name << " failed!\n";
			return false;
		}
		//boost::filesystem::file_size()获取文件大小
		int64_t fsize = boost::filesystem::file_size(name);
		body->resize(fsize);
		fs.read(&(*body)[0], fsize);//因为body是一个指针,先解引用
		if (fs.good() == false)
		{
			std::cout << "file " << name << "read data failed!\n";
			return false;
		}
		fs.close();
		return true;
	}
	static bool Write(const std::string &name, const std::string &body)
	{
		//输出流--ofstream默认打开文件的时候会清空原文件所有内容
		//覆盖写入
		std::ofstream ofs(name, std::ios::binary);//打开的时name文件，最后写入name文件
		if (ofs.is_open() == false)
		{
			std::cout << "open file" << name << " failed\n";
			return false;
		}
		ofs.write(&body[0], body.size());//将body里面的数据写入文件
		if (ofs.good() == false)
		{
			std::cout << "file " << name << " write data filed!\n";
			return false;
		}
		ofs.close();
		return true;
	}
};

class DataManager
{
	std::string _store_file;//持久化存储名称
	std::unordered_map<std::string, std::string>_backup_list;//备份
public:
	DataManager(const std::string &filename) :_store_file(filename)
	{}
	~DataManager()
	{}
	bool Insert(const std::string &key, const std::string &val)//文件名和etag信息
	{
		_backup_list[key] = val;
		Storage();
		return true;
	}
	bool GetEtag(const std::string  &key,std::string *val)
	{
		auto it = _backup_list.find(key);
		if (it == _backup_list.end())
		{
			return false;
		}
		*val = it->second;
		return true;
	}
	bool Storage()//持久化存储
	{
		//持久化存储需要，先组织数据序列化，filename etage\r\n
		std::stringstream tmp;
		auto it = _backup_list.begin();
		for (; it != _backup_list.end(); ++it)
		{
			tmp << it->first << " " << it->second << "\r\n";
		}
		FileUtil::Write(_store_file, tmp.str());
		return true;
	}
	bool InitLoad()
	{
		//启动时初始化加载原有数据	
		std::string body;
		if (FileUtil::Read(_store_file, &body) == false)
		{
			return false;
		}
		std::vector<std::string>list;
		boost::split(list, body, boost::is_any_of("\r\n"), boost::token_compress_off);
		for (auto &i : list)
		{
			size_t pos = i.find(" ");
			if (pos == std::string::npos)
			{
				continue;
			}
			std::string key = i.substr(0, pos);
			std::string val = i.substr(pos + 1);
			Insert(key, val);
		}
	}
};

//目录监控模块
class CloudClient
{
	std::string _listen_dir;//监控的指定目录
	DataManager data_manage;
	std::string _srv_ip;
	uint16_t _srv_port;
public:
	CloudClient(const std::string &filename,const std::string &store_file,const std::string &srv_ip,boost::uint16_t srv_port) : 
		_listen_dir(filename),data_manage(store_file),_srv_ip(srv_ip),_srv_port(srv_port){}
	bool Start()
	{
		data_manage.InitLoad();
		std::vector<std::string>list;
		
		while (1)
	{
		GetBackupFileList(&list);//获取到所有需要备份的文件名称
		for (int i = 0; i < list.size(); i++)
		{
			std::string name = list[i];//纯文件名
			std::string pathname = _listen_dir + name;//文件路径名
			std::cout << pathname << "is need to backup\n";
			//读取文件作为请求正文
			std::string body;
			FileUtil::Read(pathname, &body);
			httplib::Client client(_srv_ip.c_str(), _srv_port);//服务端的地址和服务端的端口
			std::string req_path = "/" + name;//得到请求路径
			auto rsp = client.Put(req_path.c_str(), body,"application/octet-stream");
			//二进制流文件上传至请求路径
			if (rsp == NULL || (rsp!=NULL && rsp->status != 200))
			{
				std::cout << pathname << "backup failed!\n";
				//文件上传失败了，不插入数据，下次循环的时候再次插入
				continue;
			}
			std::string etag;
			GetEtag(pathname, &etag);
			data_manage.Insert(name, etag);
			//备份成功则插入，更新信息
			std::cout << pathname << " backup success\n";
		}
		Sleep(1000);//休眠1000ms重新检测
	}
		return true;
}
	bool GetBackupFileList(std::vector<std::string>*list)
	{//1、进行目录进监控，获取指定目录下所有文件名称
	 //2、逐个文件计算自身当前etag
	 //3、与data_manage中保存的原有etag进行对比
		//1.没有找到原有etag进行对比
		//2.找到原有etag，但是当前etag和原有etag不相等，需要备份
		//3.找到原有etag，并且与当前etag相等则不需要备份
		if (boost::filesystem::exists(_listen_dir) == false)
		{
			boost::filesystem::create_directory(_listen_dir);
			//文件路径不存在则创建文件路径
		}
		boost::filesystem::directory_iterator begin(_listen_dir);
		boost::filesystem::directory_iterator end;
		for (; begin != end; ++begin)
		{
			if (boost::filesystem::is_directory(begin->status()))
			{
				//遇到目录直接越过
				continue;
			}
			std::string pathname = begin->path().string();
			std::string name = begin->path().filename().string();
			std::string cur_etag,old_etag;
			GetEtag(pathname, &cur_etag);//计算当前etag
			data_manage.GetEtag(name, &old_etag);//已备份文件的etag
			if (cur_etag != old_etag)
			{
				list->push_back(name);//将qi放进需要备份列表中
			}
		}
		return true;
	}
	static bool GetEtag(std::string &pathname, std::string *etag)//计算文件的etag信息
	{// etag:文件大小-文件最后一次修改时间
		//不希望被外界使用
		int64_t fsize = boost::filesystem::file_size(pathname);//文件大小
		time_t mtime = boost::filesystem::last_write_time(pathname);//文件最后一次修改时间
		*etag = std::to_string(fsize) + "-" + std::to_string(mtime);
		return true;
	}
};