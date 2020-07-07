#include<cstdio>
#include<string>
#include<vector>
#include<fstream>
#include<unordered_map>
#include<zlib.h>
#include<pthread.h>
#include<boost/filesystem.hpp>
#include<boost/algorithm/string.hpp>
#include"httplib.h"

#define NONHOT_TIME 10//10s未访问，认为是非热点文件
#define INTERVAL_TIME 30//非热点检测间隔时间
#define BACKUP_DIR "./backup/"//文件备份路径
#define GZFILE_DIR "./gzfile/"//压缩包存放路径
#define DATA_FILE "./listbackup"//数据管理模块的数据备份文件路径
namespace _cloud_sys
{

  class FileUtil
  {
    public:
   static bool Read(const std::string &name,std::string *body)
   { //从文件中读取所有内容
     std::ifstream fs(name,std::ios::binary);
       if(fs.is_open() == false)
       {
         std::cout<<"open file "<<name<<"false\n";
         return false;
       }
       //boost::filesystem::file_size()获取文件大小
     int64_t fsize=boost::filesystem::file_size(name);  
      body->resize(fsize);
      fs.read(&(*body)[0],fsize);//因为body是一个指针,先解引用
      if(fs.good() == false)
    {
       std::cout<<"file "<<name<<"read data failed!\n";
       return false;
    }
    fs.close();
    return true;
    }
  static bool Write(const std::string &name,const std::string &body)
  {
    //输出流--ofstream默认打开文件的时候会清空原文件所有内容
    //覆盖写入
    std::ofstream ofs(name,std::ios::binary);//打开的时name文件，最后写入name文件
    if(ofs.is_open() == false)
    {
      std::cout<<"open file"<<name<<" failed\n";
      return false;
    }
    ofs.write(&body[0],body.size());//将body里面的数据写入文件
    if(ofs.good() == false)
    {
    std::cout<<"file "<<name<<" write data filed!\n";
    return false;
    }
    ofs.close();
    return true;
   }
  };
  class CompressUtil
{
  public:
      static bool Compress(const std::string &src,const std::string &dst)
   {//文件压缩-源文件名称-压缩包名称
        std::string body;
        FileUtil::Read(src,&body);//将源文件读到body里
      gzFile gf=gzopen(dst.c_str(),"wb");//打开压缩包,gzip文件
      if(gf == NULL)
      {
      std::cout<<"open file "<<dst<<"false!\n";
        return false;
      }
      int wlen=0;
      //若一次没有将全部数据压缩，则进行多次压缩（压缩写入）
      while(wlen < body.size())
      {
        int ret=gzwrite(gf,&body[wlen],body.size()-wlen);
        if(ret == 0)
        {
          std::cout<<"file "<<dst<<"write compress data filed!\n";
            return false;
        }
        wlen += ret;
      }
      gzclose(gf);
      return true;
    }
      static bool UnCompress(const std::string &src,const std::string &dst)
      {   //文件解压缩-压缩包名称-源文件名称
        //普通文件
        std::ofstream ofs(dst,std::ios::binary);
        if(ofs.is_open() == false)
        {
          std::cout<<"open file "<<dst<<"failed!\n";
            return false;
        }
        gzFile gf =gzopen(src.c_str(),"rb");//以读的方式打开
        if(gf == NULL)
        {
          std::cout<<"open file "<<src<<"failed!\n";
          ofs.close();//关闭上面打开的文件
          return false;
        }
        char tmp[4096] = {0};
        int ret;
        while((ret = gzread(gf,tmp,4096)) >0)
          //gzread(句柄，缓冲区，缓冲区大小)
        {//循环读直至将数据全部读完
          ofs.write(tmp,ret);
        }//读多少，写多少
        ofs.close();
        gzclose(gf);
          return true;
      }
};

  class DataManager
  {
      public:
        DataManager(const std::string &path): _back_file(path)
          {
          pthread_rwlock_init(&_rwlock,NULL);//初始化读写锁
          }
        ~DataManager()
        {
         pthread_rwlock_destroy(&_rwlock);
        }
        bool Exists(const std::string &name)
        //判断文件是否存在
      {
        //是否能从_file_list中找到这个信息
       pthread_rwlock_rdlock(&_rwlock);
        auto it = _file_list.find(name);
        if(it == _file_list.end())
        {
          pthread_rwlock_unlock(&_rwlock);
          return false;
        }
          pthread_rwlock_unlock(&_rwlock);
        return true;
      }
        bool IsCompress(const std::string &name)
        //判断文件是否已经压缩
        {
         pthread_rwlock_rdlock(&_rwlock);
          auto it = _file_list.find(name);
          if(it == _file_list.end())
          {
          pthread_rwlock_unlock(&_rwlock);
            return false;
          }
          if(it->first == it->second)
         //名称一致表示未压缩
          {
          pthread_rwlock_unlock(&_rwlock);
            return false;
          }
          pthread_rwlock_unlock(&_rwlock);
          return true;
        }
        bool NonCompressList(std::vector<std::string>*list)
        //获取未压缩文件序列
        {
          pthread_rwlock_rdlock(&_rwlock);  
          auto it = _file_list.begin();
          for( ;it != _file_list.end(); ++it )
          {
            if(it->first == it->second)
            {
              list->push_back(it->first);
            }
          }
          pthread_rwlock_unlock(&_rwlock);
          return true;
        }
        bool Insert(const std::string &src,const std::string &dst)
        //插入或更新数据
        {
          pthread_rwlock_wrlock(&_rwlock);
        _file_list[src] = dst; 
          pthread_rwlock_unlock(&_rwlock);
          Storage();
        return true;
        }
        bool GetAllName(std::vector<std::string>*list)
        //获取所有文件名称,server模块里面的获取所有文件列表使用
        {
         pthread_rwlock_rdlock(&_rwlock);
          auto it = _file_list.begin();
          for(;it != _file_list.end(); ++it)
          {
            list->push_back(it->first);
          }
         pthread_rwlock_unlock(&_rwlock);
         Storage();//更新修改之后持久化保存,将数据全部覆盖写入持久化存储
          return true;
        }
    //根据源文件名称获取压缩包名称
      bool GetGzName(const std::string &src,std::string *dst)
    {
       auto it = _file_list.find(src);
       if(it == _file_list.end())
      {
         return false;
      }
        *dst = it->second;
        return true;
    }
        bool Storage()
        //持久化存储
        {
          //持久化存储需要，先组织数据序列化，src dst\r\n
          std::stringstream tmp;       
          pthread_rwlock_rdlock(&_rwlock);
          auto it = _file_list.begin();
          for(;it != _file_list.end(); ++it)
          {
            tmp<<it->first<<" "<<it->second<<"\r\n";
          }
          pthread_rwlock_unlock(&_rwlock);
          FileUtil::Write(_back_file,tmp.str());
          //没有修改数据，加读锁就可以了
          return true;
        }
        bool InitLoad()
        //启动时初始化加载原有数据
        {
          std::string body;
         if(FileUtil::Read(_back_file,&body) == false)
         {
           return false;
         }
     std::vector<std::string>list;
   boost::split(list,body,boost::is_any_of("\r\n"),boost::token_compress_off);
       for(auto &i:list)
       {
         size_t pos = i.find(" ");
         if(pos == std::string::npos)
         {
           continue;
         }
         std::string key = i.substr(0,pos);
         std::string val = i.substr(pos+1);
         Insert(key,val);
         //将key val 添加到_file_list里
       }
        }
    private:
        std::string _back_file;//持久化存储
       std::unordered_map<std::string,std::string>_file_list;//数据管理容器
       pthread_rwlock_t _rwlock;//读写锁
  };
 _cloud_sys:: DataManager data_manage(DATA_FILE);
class NonHotCompress
{
  public:
    NonHotCompress(const std::string bu_dir,std::string gz_dir)
      :_bu_dir(bu_dir),_gz_dir(gz_dir)
    {}
    ~NonHotCompress()
    {}
    bool Start()
    //总体向外提供的功能接口，开始压缩模块
    {
      while(1)
      {
         std::vector<std::string>list;
         data_manage.NonCompressList(&list);
         for(int i = 0;i < list.size();i++)
         {
          bool ret = FileIsHot(list[i]);
          if(ret == false)
          {
            std::cout<<"no Hot File"<<list[i]<<std::endl; 
      std::string s_filename = list[i];//文件原名称
      std::string d_filename = list[i]+".gz";//目标压缩包文件名称
     std::string src_name = _bu_dir + s_filename;//备份路径+源文件名称
     std::string dst_name = _gz_dir + d_filename;//压缩包名称带路径的
           if( CompressUtil::Compress(src_name,dst_name) == true)
          {
            data_manage.Insert(s_filename , d_filename);//更新数据信息 
            unlink(src_name.c_str());//删除源文件
          }
          }
         }
         sleep(INTERVAL_TIME);
      }
      return true;
    }
  private:
  bool FileIsHot(const std::string &name)
  //判断一个文件是否为热点文件
  {
    time_t cur_t = time(NULL);
    struct stat st;
    if(stat(name.c_str(),&st) < 0)
    {
      std::cout<<"get file"<<name<<"start failed!\n";
      return false;
    }
    if((cur_t - st.st_atime) > NONHOT_TIME)
    {
      return false;
    }
    return true;//非热点返回false，热点返回true
  }
  private:
    std::string _bu_dir;//压缩前文件存储路径
    std::string _gz_dir;//压缩后文件的存储路径
};
class Server
{
  public:
    Server()
    {}
    ~Server()
    {}
    bool Start()
    //启动网络通信模块接口
    {
      data_manage.InitLoad();
      _server.Put("/(.*)",Upload);
      _server.Get("/list",List);
      _server.Get("/downLoad/(.*)",Download);
      //添加路由表
      _server.listen("0.0.0.0",9000);//搭建tcp服务器，进行http数据接收处理
      return true;
    }
  private:
    static void Upload(const httplib::Request &req,httplib::Response &rsp)
    //上传文件备份
    //可以从req里解析出：
    //req.method 请求方法
    //req.path 请求资源路径
    //req.headers 头部信息键值对
    //req.body 存放请求数据的正文
    {
      std::string filename = req.matches[1];//捕获的文件名
      std::string pathname = BACKUP_DIR + filename;//组织带路径的文件名
      FileUtil::Write(pathname,req.body);//文件存储成功
      rsp.status=200;
      data_manage.Insert(filename,filename);//数据管理模块添加文件信息
      return;
    }
    static void List(const httplib::Request &req,httplib::Response &rsp)
    //文件列表处理回调函数
    {
      std::vector<std::string>list;
      data_manage.GetAllName(&list);
      std::stringstream tmp;
      tmp << "<html><body><hr />";
      for(int i = 0;i < list.size(); i++)
      {
        tmp << "<a href='/download/"<< list[i] << "'>"<<list[i] << "</a>";
     //tmp<<"<a href=/download/a.txt">a.txt</a>
     tmp << "<hr />";
      }
      tmp<<"<hr /></body></html>";

      rsp.set_content(tmp.str().c_str(),tmp.str().size(),"test/html");
      //正文数据，正文长度，正文类型
      rsp.status = 200;
      return;
    }
  static void Download(const httplib::Request &req,httplib::Response &rsp)
    //文件下载处理回调函数
  {
    std::string filename = req.matches[1];
    if(data_manage.Exists(filename) == false)
    {
      rsp.status = 404;//文件不存在
      return;
    }
    std::string pathname = BACKUP_DIR + filename;
    if(data_manage.IsCompress(filename) != false)
    {
      //已压缩，找到压缩包，得到源文件名称，再将压缩包解压至源文件名称处
    std::string gzfile;
    data_manage.GetGzName(filename,&gzfile);
    std::string gzpathname = GZFILE_DIR + gzfile;
    //组织带路径的压缩报文件名称
    CompressUtil::UnCompress(gzpathname,pathname);
    unlink(gzpathname.c_str());//删除压缩包
    data_manage.Insert(filename,filename);//更新数据管理模块
    }
      FileUtil::Read(pathname,&rsp.body);//直接将文件的数据读取到rsp的body中
      rsp.set_header("Content-Type","application/octet-stream");//二进制流下载
      rsp.status = 200;
      return;
  }
  private:
    //std::string _file_dir;//文件上传备份路径
    httplib::Server _server;
};
}
