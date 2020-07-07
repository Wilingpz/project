#pragma once
//f12 跳到函数内部实现
//ctr+-可以返回原来函数名处

#include<iostream>
#include<time.h>
#include<math.h>
#include<fstream>
#include<boost/multiprecision/cpp_int.hpp>
#include<boost/multiprecision/random.hpp>
#include<boost/multiprecision/miller_rabin.hpp>

#define NUMBER 256

//typedef long DataType;//long类型占8个字节
typedef boost::multiprecision::int1024_t DataType;
namespace brdm = boost::random;

class key
{
public:
	DataType _ekey;//加密密钥
	DataType _dkey;//解密密钥
	DataType _nkey;//(e,n)(d,n)中的n
};

/*
1、产生两个素数
2、求n
3、求f(n)
4、产生e  1<e<f(n)
5、产生d
*/
class RSA
{
	key _key;
	//这里可以将所有函数私有，只留加密和解密的两个接口
public:	
	RSA();
	void decode(const char* filename,const char* fileout);//文件存储路径，加密后文件的存储路径
	void encrypt(const char* filename, const char* fileout);
private:
	void getkey();
	DataType encrypt(DataType key, DataType ekey, DataType nkey);//加密
	DataType decode(DataType key, DataType dkey, DataType nkey);//解密
	DataType GetPrime();
	bool is_Prime(DataType prime);
	DataType Getnkey(DataType prime1, DataType prime2);
	DataType GetEuler(DataType prime1, DataType prime2);
	DataType Getekey(DataType euler);
	DataType Getdkey(DataType ekey, DataType euler);
	DataType Getgcd(DataType data1, DataType data2);
	DataType exGcd(DataType a, DataType b, DataType &x, DataType &y);
	//key getallkey();
};