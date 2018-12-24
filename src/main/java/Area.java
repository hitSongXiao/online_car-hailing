import java.io.Serializable;

public class Area implements Serializable {
    int area;
    private int change[] = new int[]{0,-1,2,-361,720,-721,2,718,2};
    private int sum[] = new int[]{0,-1,1,-360,360,-361,-359,359,361};
    private int tag = 0;

    public Area(int area){
        this.area = area;
    }
    public int getArea(){
        return area;
    }

    public int _getRealArea(){
        return area-sum[(tag++)%9];
    }

    public Area _getNextArea(){
        int t = tag;
        Area next = new Area(area+change[(t++)%9]);
        next.setTag(t);
        return next;
    }

    public void setTag(int tag){
        this.tag = tag;
    }

    public int _getTag(){
        return tag;
    }

    @Override
    public int hashCode(){
        return area;
    }

    @Override
    public boolean equals(Object obj){
        if(obj==null || !(obj instanceof Area)){
            return false;
        }
        return area==(((Area) obj).getArea());
    }


    @Override
    public String toString(){
        return "Area:"+area;
    }
}
