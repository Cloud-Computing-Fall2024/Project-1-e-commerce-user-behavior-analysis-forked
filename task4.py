from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from operator import itemgetter
from itertools import groupby

class RevenueAnalysis(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_revenue,
                  reducer=self.reducer_total_revenue),
            MRStep(reducer=self.reducer_top_products)
        ]
    
    def mapper_revenue(self, _, line):
        num_commas = line.count(',')
        
        if num_commas == 6:
            if line.startswith('TransactionID'):
                return

            try:
                row = next(csv.reader([line]))
                transaction_id, user_id, product_category, product_id, quantity_sold, revenue_generated, transaction_timestamp = row
                
                yield product_id, (float(revenue_generated), product_category)
            
            except (ValueError, IndexError) as e:
                self.increment_counter('Error', 'Parse Error', 1)

        elif num_commas == 3:
            if line.startswith('ProductID'):
                return  # Skip header row

            try:
                row = next(csv.reader([line]))
                product_id, product_name, product_category, price = row

                yield product_id, (float(price), product_category)
            
            except (ValueError, IndexError) as e:
                self.increment_counter('Error', 'Parse Error', 1)
    
    def reducer_total_revenue(self, product_id, values):
        total_revenue = 0
        category = None
        count = 0
        
        for revenue, cat in values:
            total_revenue += revenue
            category = cat
            count += 1
        
        yield category, {
            'product_id': product_id,
            'total_revenue': total_revenue,
            'no_of_products': count
        }
    
    def reducer_top_products(self, category, values):
        products = []
        
        for value in values:
            product_id = value['product_id']
            total_revenue = value['total_revenue']
            count = value['no_of_products']
            avg_revenue = total_revenue / count if count > 0 else 0
            
            products.append({
                'product_id': product_id,
                'total_revenue': total_revenue,
                'avg_revenue': avg_revenue
            })
        
        sorted_products = sorted(products, 
                                key=lambda x: x['total_revenue'], 
                                reverse=True)
        top_3 = sorted_products[:3]
        
        yield category, {
            'top_3_products': [
                {
                    "product_id": p['product_id'],
                    "total_revenue": p['total_revenue'],
                    "avg_revenue": p['avg_revenue']
                } 
                for p in top_3
            ]
        }



if __name__ == '__main__':
    RevenueAnalysis.run()