/**
 * 입력값 검증 유틸리티
 */
export class Validators {
    /**
     * 빈 값 체크
     */
    static isEmpty(value) {
        return value === null || 
               value === undefined || 
               value === '' || 
               (typeof value === 'string' && value.trim() === '');
    }

    /**
     * 이메일 검증
     */
    static isEmail(email) {
        const regex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return regex.test(email);
    }

    /**
     * 전화번호 검증 (한국)
     */
    static isPhoneNumber(phone) {
        const cleaned = phone.replace(/[^0-9]/g, '');
        return cleaned.length >= 10 && cleaned.length <= 11;
    }

    /**
     * IP 주소 검증
     */
    static isIPAddress(ip) {
        const ipv4Regex = /^(\d{1,3}\.){3}\d{1,3}$/;
        const ipv6Regex = /^([0-9a-fA-F]{0,4}:){7}[0-9a-fA-F]{0,4}$/;
        
        if (ipv4Regex.test(ip)) {
            const parts = ip.split('.');
            return parts.every(part => {
                const num = parseInt(part);
                return num >= 0 && num <= 255;
            });
        }
        
        return ipv6Regex.test(ip);
    }

    /**
     * 날짜 형식 검증 (YYYY-MM-DD)
     */
    static isDateFormat(date) {
        const regex = /^\d{4}-\d{2}-\d{2}$/;
        if (!regex.test(date)) return false;
        
        const parts = date.split('-');
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]);
        const day = parseInt(parts[2]);
        
        if (month < 1 || month > 12) return false;
        
        const daysInMonth = new Date(year, month, 0).getDate();
        return day >= 1 && day <= daysInMonth;
    }

    /**
     * Alert ID 검증
     */
    static isValidAlertId(alertId) {
        if (this.isEmpty(alertId)) return false;
        // Alert ID 형식에 맞게 수정 필요
        return alertId.length > 0;
    }

    /**
     * Customer ID 검증
     */
    static isValidCustomerId(custId) {
        if (this.isEmpty(custId)) return false;
        // Customer ID 형식에 맞게 수정 필요
        return /^[A-Za-z0-9]+$/.test(custId);
    }

    /**
     * 포트 번호 검증
     */
    static isValidPort(port) {
        const portNum = parseInt(port);
        return !isNaN(portNum) && portNum > 0 && portNum <= 65535;
    }

    /**
     * JDBC URL 검증
     */
    static isValidJdbcUrl(url) {
        return url.startsWith('jdbc:oracle:thin:@');
    }

    /**
     * 필수 필드 검증
     */
    static validateRequired(fields, data) {
        const errors = {};
        
        for (const field of fields) {
            if (this.isEmpty(data[field])) {
                errors[field] = `${field} is required`;
            }
        }
        
        return {
            isValid: Object.keys(errors).length === 0,
            errors
        };
    }

    /**
     * 데이터베이스 연결 정보 검증
     */
    static validateDBConnection(data, dbType = 'oracle') {
        const errors = {};
        
        if (dbType === 'oracle') {
            if (this.isEmpty(data.host)) errors.host = 'Host is required';
            if (this.isEmpty(data.port)) errors.port = 'Port is required';
            else if (!this.isValidPort(data.port)) errors.port = 'Invalid port number';
            if (this.isEmpty(data.service_name)) errors.service_name = 'Service name is required';
            if (this.isEmpty(data.username)) errors.username = 'Username is required';
        } else if (dbType === 'redshift') {
            if (this.isEmpty(data.host)) errors.host = 'Host is required';
            if (this.isEmpty(data.port)) errors.port = 'Port is required';
            else if (!this.isValidPort(data.port)) errors.port = 'Invalid port number';
            if (this.isEmpty(data.dbname)) errors.dbname = 'Database name is required';
            if (this.isEmpty(data.username)) errors.username = 'Username is required';
        }
        
        return {
            isValid: Object.keys(errors).length === 0,
            errors
        };
    }

    /**
     * 날짜 범위 검증
     */
    static validateDateRange(startDate, endDate) {
        if (!this.isDateFormat(startDate)) {
            return { isValid: false, error: 'Invalid start date format' };
        }
        
        if (!this.isDateFormat(endDate)) {
            return { isValid: false, error: 'Invalid end date format' };
        }
        
        const start = new Date(startDate);
        const end = new Date(endDate);
        
        if (start > end) {
            return { isValid: false, error: 'Start date cannot be after end date' };
        }
        
        return { isValid: true };
    }

    /**
     * 숫자 범위 검증
     */
    static isInRange(value, min, max) {
        const num = parseFloat(value);
        if (isNaN(num)) return false;
        return num >= min && num <= max;
    }

    /**
     * 문자열 길이 검증
     */
    static isLengthValid(value, minLength, maxLength) {
        if (typeof value !== 'string') return false;
        const length = value.length;
        return length >= minLength && length <= maxLength;
    }
}