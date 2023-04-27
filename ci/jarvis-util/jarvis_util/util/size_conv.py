class SizeConv:
    @staticmethod
    def to_int(text):
        text = text.lower()
        if 'k' in text:
            return SizeConv.KB(text)
        if 'm' in text:
            return SizeConv.MB(text)
        if 'g' in text:
            return SizeConv.GB(text)
        if 't' in text:
            return SizeConv.TB(text)
        if 'p' in text:
            return SizeConv.PB(text)
        return int(text)

    @staticmethod
    def KB(num):
        return int(num.split('k')[0]) * (1 << 10)

    @staticmethod
    def MB(num):
        return int(num.split('m')[0]) * (1 << 20)

    @staticmethod
    def GB(num):
        return int(num.split('g')[0]) * (1 << 30)

    @staticmethod
    def TB(num):
        return int(num.split('t')[0]) * (1 << 40)

    @staticmethod
    def PB(num):
        return int(num.split('p')[0]) * (1 << 50)
