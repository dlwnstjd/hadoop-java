package reducetest;

public class CarrierCode {
	private String carrierCode;
	private String carrierName;
	public CarrierCode(String value) {	//생성자
		try {
			String[] columns = value.split(",");
			if(columns != null && columns.length > 0) {
				carrierCode = columns[0];
				carrierName = columns[1];
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	public String getCarrierCode() {
		return carrierCode;
	}
	public String getCarrierName() {
		return carrierName;
	}	
}
