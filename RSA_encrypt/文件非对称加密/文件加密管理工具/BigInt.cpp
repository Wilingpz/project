#include"BIgInt.h"


BigInt::BigInt(const std::string &num)//这里传const表示直接传字符串进来
	:_number(num)
	{}//拷贝构造过来，是深拷贝？？


BigInt BigInt::operator+(BigInt &num)
{
	std::string tmp = add(_number, num._number);
	return BigInt(tmp);
}
BigInt BigInt::operator-(BigInt &num)
{
	std::string tmp = sub(_number, num._number);
	return BigInt(tmp);
}
BigInt BigInt:: operator*(BigInt &num)
{
	std::string tmp = mul(_number, num._number);
	return BigInt(tmp);

}
BigInt BigInt:: operator/(BigInt &num)
{
	std::pair<std::string,std::string> tmp = dev(_number, num._number);
	return BigInt(tmp.first);

}
BigInt BigInt:: operator%(BigInt &num)
{
	std::pair<std::string, std::string> tmp = dev(_number, num._number);
	return BigInt(tmp.second);
}

//模拟运算的过程
std::string BigInt::add(std::string num1, std::string num2)
{
	//补齐操作
	int len1 = num1.size();
	int len2 = num2.size();
	int diffNum = abs(len1 - len2);
	int longsize = len1 > len2 ? len1 : len2;
	if (len1 < len2)
	{
		num1.insert(0, diffNum, '0');//给前面补零
	}
	else if (len1>len2)
	{
		num2.insert(0, diffNum, '0');
	}
	std::string ret;//保存结果
	ret.resize(longsize);//operator[]
	//逐位相加
	int step = 0;//进位
	for (int i = longsize - 1; i >= 0; i--)
	{
		ret[i] = (num1[i] - '0') + (num2[i] - '0')+step;//这里要-0化成数字相加
		ret[i] += '0';
		//更新
		if (ret[i] > '9')
		{
			ret[i] -= 10;//相当于阿斯卡码往前退10个
			step = 1;
		}
		else
		{
			step = 0;
		}
	}
	if (step == 1)
	{
		ret.insert(0, 1, '1');
	}
	return ret;
}

std::string BigInt::sub(std::string num1, std::string num2)
{
	int len1 = num1.size();
	int len2 = num2.size();
	int diffNum = abs(len1 - len2);
	int longsize = len1 > len2 ? len1 : len2;
	if (len1 < len2)
	{
		num1.insert(0, diffNum, '0');//给前面补零
	}
	else if (len1>len2)
	{
		num2.insert(0, diffNum, '0');
	}
	std::string ret;
	ret.resize(longsize);
	for (int i = longsize - 1; i >= 0; --i)
	{
		if (num1[i] < num2[i])
		{
			num1[i] += 10;
			//更新高位
			num1[i - 1]--;
		}
		ret[i] = (num1[i] - '0')-(num2[i]-'0')+'0';//变成数字进行相减;相减完成后要变回字符
	}
	//删除前置0
	while (ret.size()>1&&ret[0]=='0')//至少有一位数且最位为0
	{
		ret.erase(0, 1);//删除最高位的1
	}
	return ret;
}
std::string BigInt::mul(std::string num1, std::string num2)
{
	 //简化乘法的过程  比较小的数*比较大的数
	if (num2.size() > num1.size())
	{
		swap(num2, num1);//保证num2是较小的数
	}
	std::string ret = "0";//ret:'\0'(如果不写的话)用于存放结果
	for (int i = num2.size() - 1; i >= 0; --i)
	{
    //获取当前乘数对应的值
		int curnum2 = num2[i] - '0';
		int step = 0;//进位
		//mum1不可以改变所以这里找了temp来存放当前位的乘积结果
		std::string temp = num1;
		for (int j = num1.size() - 1; j >= 0; --j)
		{
			//诸位相乘
			temp[j] = (temp[j] - '0')*curnum2 + step;//整数值  最大9*9+9
			if (temp[j] > 9)
			{
				step = temp[j] / 10;
				temp[j] = temp[j] - step * 10;
			}
			else
			{
				step = 0;
			}
			temp[j] += '0';//还原为字符
	    }
		//最后判断是否需要进位
		if (step > 0)
		{
			temp.insert(0, 1, step + '0');//在第0个位置插入一个数，step+'0'
		}
		//补零
		temp.append((num2.size() - 1) - i, '0');//补多少个0
		//累加一次加法的结果
		ret = add(ret, temp);
	}
	return ret;
}
std::pair<std::string, std::string> BigInt::dev(std::string num1, std::string num2)//返回商和余数
{
	std::string ret;//商
	std::string rem = num1;//余数，还没有进行操作
	//给除数放大
	int diffNum = num1.size() - num2.size();//默认被除数比除数大
	num2.append(diffNum, '0');
	for (int i = 0; i <= diffNum; i++)//被除数减不同除数的操作要执行的次数
	{
		//记录减法执行的次数
		char count = '0';
		while (1)
		{
			if (less(rem, num2))//判断减数是否大于被减数
			{
				break;//减不开了就break
			}
			rem = sub(rem, num2);//被除数-除数
			++count;
		}
		ret += count;
		//除数减小十倍
		num2.pop_back();
	}
	//删除前置0
	while (ret.size() > 1 && ret[0] == '0')
	{
		ret.erase(0, 1);
	}
	//返回<商，余数>
	return make_pair(ret, rem);//无需写出型别，就可以生成一个pair对象
}
bool BigInt::less(std::string& num1, std::string& num2)
{
	if (num1.size() < num2.size())//长度不相同
	{
		return true;
	}
	if (num1.size() > num2.size())//长度不相同
	{
		return false;
	}
	//长度相同时,字符串比较阿斯卡码值
	return num1 < num2;
}
std::ostream& operator<<(std::ostream& _cout, BigInt &num)
{
	_cout << num._number << std::endl;
	return _cout;
}